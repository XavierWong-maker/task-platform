"""工作流 DAG 的静态结构：节点、依赖、校验与拓扑排序。

纯内存、不依赖数据库和执行器。WorkflowDefinition 一旦构造成功就保证是合法的 DAG：
节点名唯一、依赖都存在、没有自依赖、没有环。

- layers()：按依赖深度分层，同一层的节点互不依赖，可以并行执行
- topological_order()：layers() 展平后的线性顺序
- 所有输出都按节点声明顺序稳定排序，保证结果可复现
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass


class WorkflowValidationError(ValueError):
    """工作流定义不合法"""


class CyclicDependencyError(WorkflowValidationError):
    """工作流存在循环依赖；cycle 为环路径，首尾相同"""

    def __init__(self, cycle: list[str]) -> None:
        super().__init__("存在循环依赖: " + " -> ".join(cycle) + "（箭头表示“依赖于”）")
        self.cycle = cycle


@dataclass(frozen=True, slots=True)
class Dependency:
    """一条依赖边：downstream 必须等 upstream 成功后才能运行"""

    upstream: str
    downstream: str


@dataclass(frozen=True, slots=True)
class TaskNode:
    """工作流中的一个节点：name 在工作流内唯一，job 是要执行的 Job.name"""

    name: str
    job: str
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise WorkflowValidationError("节点名称不能为空")
        if not self.job.strip():
            raise WorkflowValidationError(f"节点 {self.name} 的 job 不能为空")
        if self.name in self.depends_on:
            raise WorkflowValidationError(f"节点 {self.name} 不能依赖自己")
        if len(set(self.depends_on)) != len(self.depends_on):
            raise WorkflowValidationError(f"节点 {self.name} 的依赖中有重复项")


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    name: str
    nodes: tuple[TaskNode, ...]

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise WorkflowValidationError("工作流名称不能为空")
        if not self.nodes:
            raise WorkflowValidationError("工作流至少需要一个节点")

        counts = Counter(n.name for n in self.nodes)
        duplicated = sorted(name for name, c in counts.items() if c > 1)
        if duplicated:
            raise WorkflowValidationError(f"节点名称重复：{duplicated}")

        for node in self.nodes:
            missing = [d for d in node.depends_on if d not in counts]
            if missing:
                raise WorkflowValidationError(f"节点 {node.name} 依赖了不存在的节点：{missing}")

        self.layers()  # 有环则在构造时就抛出 CyclicDependencyError

    # ---------------- 查询 ----------------

    def node(self, name: str) -> TaskNode:
        for n in self.nodes:
            if n.name == name:
                return n
        raise KeyError(name)

    def dependencies(self) -> list[Dependency]:
        """所有依赖边，按下游节点声明顺序、再按 depends_on 顺序"""
        return [Dependency(up, n.name) for n in self.nodes for up in n.depends_on]

    def roots(self) -> list[str]:
        """没有任何依赖的节点（起点）"""
        return [n.name for n in self.nodes if not n.depends_on]

    def _children(self) -> dict[str, list[str]]:
        children: dict[str, list[str]] = {n.name: [] for n in self.nodes}
        for dep in self.dependencies():
            children[dep.upstream].append(dep.downstream)
        return children

    # ---------------- 拓扑排序 ----------------

    def layers(self) -> list[list[str]]:
        """Kahn 算法按层输出：同一层的节点互不依赖，可以并行。

        每一层内按声明顺序排列。存在环时抛 CyclicDependencyError。
        """
        index = {n.name: i for i, n in enumerate(self.nodes)}
        pending = {n.name: len(n.depends_on) for n in self.nodes}
        children = self._children()

        layers: list[list[str]] = []
        placed = 0
        current = [n.name for n in self.nodes if pending[n.name] == 0]
        while current:
            layers.append(current)
            placed += len(current)
            ready: list[str] = []
            for name in current:
                for child in children[name]:
                    pending[child] -= 1
                    if pending[child] == 0:
                        ready.append(child)
            current = sorted(ready, key=index.__getitem__)

        if placed != len(self.nodes):
            remaining = {name for name, count in pending.items() if count > 0}
            raise CyclicDependencyError(self._find_cycle(remaining))
        return layers

    def topological_order(self) -> list[str]:
        return [name for layer in self.layers() for name in layer]

    def _find_cycle(self, remaining: set[str]) -> list[str]:
        """remaining 里的每个节点至少依赖 remaining 中的另一个节点，
        沿着依赖一路走下去，必然会回到走过的节点，这段就是一个环"""
        by_name = {n.name: n for n in self.nodes}
        node = next(n.name for n in self.nodes if n.name in remaining)
        position: dict[str, int] = {}
        path: list[str] = []
        while node not in position:
            position[node] = len(path)
            path.append(node)
            node = next(d for d in by_name[node].depends_on if d in remaining)
        return [*path[position[node] :], node]

    def downstream_of(self, name: str) -> list[str]:
        """name 的所有传递下游（不含自己），按拓扑顺序返回；失败传播会用到"""
        self.node(name)  # 不存在则抛 KeyError
        children = self._children()
        seen: set[str] = set()
        queue = deque(children[name])
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            queue.extend(children[current])
        return [n for n in self.topological_order() if n in seen]
