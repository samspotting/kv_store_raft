from abc import ABC
try:
   import cPickle as pickle
except:
   import pickle

class BaseSchema(ABC):

    KEY = -1
    NON_REPR_FIELDS = set()

    def __repr__(self):
        fields = ""
        for key, value in sorted(self.__dict__.items()):
            if key in self.NON_REPR_FIELDS:
                continue
            fields += f"{key}={value}, "
        return self.__class__.__name__ + "(" + fields.rstrip(",") + ")"
    
    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return self.__dict__ == other.__dict__

    @classmethod
    def is_valid(cls, msg):
        return msg[:len(cls.__name__) + 1] == cls.__name__ + " "

    @classmethod
    def is_type(cls, msg):
        return int.from_bytes(msg[:1], byteorder='big') == cls.KEY

    def serialize(self):
        return bytes([self.KEY]) + pickle.dumps(self)

    @classmethod
    def deserialize(cls, msg):
        return pickle.loads(msg[1:])