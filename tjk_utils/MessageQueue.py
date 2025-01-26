import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
from collections import deque
import json
from telebot.async_telebot import AsyncTeleBot
import pytz

class MessagePriority(Enum):
    CRITICAL = 0    # System errors, critical alerts
    HIGH = 1        # Trade signals, position updates
    MEDIUM = 2      # Risk updates, performance metrics
    LOW = 3         # Regular status updates, market info

@dataclass
class QueuedMessage:
    priority: MessagePriority
    content: str
    timestamp: datetime
    attempts: int = 0
    max_attempts: int = 3
    tags: List[str] = None

    def __lt__(self, other):
        return self.priority.value < other.priority.value

class MessageQueue:
    def __init__(self, max_size: int = 1000):
        self.queue = deque(maxlen=max_size)
        self.processing = False
        self._lock = asyncio.Lock()

    async def put(self, message: QueuedMessage):
        async with self._lock:
            self.queue.append(message)
            # Sort by priority (lower value = higher priority)
            sorted_queue = sorted(self.queue)
            self.queue.clear()
            self.queue.extend(sorted_queue)

    async def get(self) -> Optional[QueuedMessage]:
        async with self._lock:
            return self.queue.popleft() if self.queue else None

    def is_empty(self) -> bool:
        return len(self.queue) == 0