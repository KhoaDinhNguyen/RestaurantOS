"""Health-state transitions, with no I/O."""

from dataclasses import dataclass
from datetime import datetime

from restaurantos.common.localnode import NodeView
from restaurantos.coordinator.models import NodeStatus


@dataclass
class NodeHealth:
    response: NodeStatus
    misses: int = 0

    def observe(
        self, view: NodeView, at: datetime, threshold: int, gateway_reachable: bool | None
    ) -> list[str]:
        """Count a heartbeat, then apply an optional independent observation."""
        previous = self.response.status
        if view.reachable:
            self.misses = 0
            self.response.status = "up"
            self.response.role = "standby" if view.in_recovery else "primary"
            self.response.last_heartbeat_at = at
        else:
            self.misses += 1
            if self.misses >= threshold and previous != "down":
                self.response.status = "suspect"

        events = []
        if self.response.status != previous:
            events.append(f"node_{self.response.status}")
        if not view.reachable and self.misses >= threshold:
            events.extend(self.confirm_gateway(gateway_reachable))
        return events

    def confirm_gateway(self, reachable: bool | None) -> list[str]:
        if reachable is None:
            return []
        previous = self.response.status
        self.response.status = "suspect" if reachable else "down"
        return [] if self.response.status == previous else [f"node_{self.response.status}"]
