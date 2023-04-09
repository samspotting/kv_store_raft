from enum import Enum
import os
import uuid

from library.utils import split_and_pad
from schema.base_schema import BaseSchema
from library.logging import get_logger

logger = get_logger(os.path.basename(__file__))

class Status(Enum):
    SUCCESS = 1
    FAIL = 2
    TIMEOUT = 3
    REDIRECT = 4
    NOT_FOUND = 5

class Action(Enum):
    GET = 1
    SET = 2
    DELETE = 3

class KVStoreRequest(BaseSchema):
    KEY = 0

    def __init__(self, action, key=None, value=None, id=None):
        # validation
        if action not in (Action.GET, Action.SET, Action.DELETE)\
                or (action == Action.GET and (not key or value))\
                or (action == Action.SET and not (key or value))\
                or (action == Action.DELETE and (not key or value)):
            raise ValueError("Invalid request")
        self._id = str(uuid.uuid4()) if id is None else id
        self._action = action
        self._key = key
        self._value = value

    @classmethod
    def build_from_msg(self, msg):
        command, key, value = split_and_pad(msg, " ", 3)
        return KVStoreRequest(Action[command.upper()], key, value)

    def __repr__(self):
        if self._action == Action.GET or self._action == Action.DELETE:
            return f"{self._id}{self._action.value}{self._key}"
        elif self._action == Action.SET:
            return f"{self._id}{self._action.value}{self._key}>{self._value}"
    
    @classmethod
    def from_serialized_msg(cls, msg):
        id = msg[:36]
        action = Action(int(msg[36]))
        if action == Action.GET or action == Action.DELETE:
            return KVStoreRequest(action, msg[37:], id=id)
        elif action == Action.SET:
            key, value = msg[37:].split(">")
            return KVStoreRequest(action, key, value, id=id)
        else:
            raise ValueError("Invalid format")

    @property
    def id(self):
        return self._id

    def is_get(self):
        return self._action == Action.GET

    def is_set(self):
        return self._action == Action.SET

    def is_delete(self):
        return self._action == Action.DELETE

    @property
    def value(self):
        return self._value

class KVStoreResponse(BaseSchema):
    KEY = 1

    def __init__(self, status, msg=None):
        self._status = status
        self._msg = msg

    def __repr__(self):
        return f"{self._status.name} '{self._msg}'" if self._msg is not None else self._status.name

    @property
    def msg(self):
        return self._msg

    def is_success(self):
        return self._status == Status.SUCCESS

    def is_fail(self):
        return self._status == Status.FAIL

    def is_timeout(self):
        return self._status == Status.TIMEOUT

    def is_redirect(self):
        return self._status == Status.REDIRECT

    def is_not_found(self):
        return self._status == Status.NOT_FOUND
