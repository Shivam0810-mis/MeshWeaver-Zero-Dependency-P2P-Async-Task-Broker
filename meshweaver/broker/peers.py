from dataclasses import dataclass
from math import isfinite

@dataclass(frozen=True)
class PeerInfo:
    peer_id: str
    cpu_usage: float
    running_tasks: int = 0
    max_tasks: int = 1
    is_online: bool = True


    def __post_init__(self):
        if not self.peer_id.strip():
            raise ValueError("Peer Id connot be Empty:")

        if (
            not isfinite(self.cpu_usage)
            or not 0 <= self.cpu_usage <=100
        ):
            raise ValueError(
                "CPU usage must be a finite number from 0 to 100."
            )

        if self.running_tasks < 0:
            raise ValueError("Running tasks connot be negative.")

        if self.max_tasks < 1:
            raise ValueError("Max tasks must be at least 1.")     