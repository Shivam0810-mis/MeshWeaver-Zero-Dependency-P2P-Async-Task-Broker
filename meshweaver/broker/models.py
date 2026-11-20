from dataclasses import dataclass , field
from datetime import datetime ,timezone
from enum import Enum
from typing import Any, Dict, Tuple , Optional
from uuid import uuid4


class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    ASSIGNING = "ASSIGNING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    RETRY_WAIT ="RETRY_WAIT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


@dataclass
class Task:
    name: str
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict) 

    task_id: str = field(
        default_factory=lambda: str(uuid4()),
        init=False,      
    )
    status: TaskStatus = field(
        default = TaskStatus.QUEUED,
        init=False,
    ) 
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )

    attempt_count: int = field(default=0, init=False)
    max_attempts: int = field(default=3, init=False)
    last_error: Optional[str] = field(default=None, init=False)    




