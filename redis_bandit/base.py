from typing import (
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, PrivateAttr
from walrus import Database, Hash, Lock
from walrus import Set as WalrusSet
from walrus.containers import chainable_method


class TypedHash:
    def __init__(self, database: Database, key: str, field_types: Dict[str, Type[Any]]):
        self.database = database
        self.key = key
        self._field_types = field_types

    @property
    def _lazy_fields(self) -> WalrusSet:
        if not hasattr(self, "__lazy_fields"):
            self.__lazy_fields = self.database.Set(f"{self.key}:fields")
        return self.__lazy_fields

    def _convert(self, field: str, value: str) -> Any:
        return self._field_types[field](value)

    def _get_field_key(self, field: str) -> Any:
        return f"{self.key}:{field}"

    def __getitem__(self, field: Union[str, Sequence[str]]) -> Union[Any, List[Any]]:
        if isinstance(field, str):
            return self._convert(field, self.database.get(self._get_field_key(field)))
        elif isinstance(field, (list, tuple)):
            values = self.database.mget(
                [self._get_field_key(field_) for field_ in field]
            )
            return [
                self._convert(field_, value) for field_, value in zip(field, values)
            ]
        else:
            raise ValueError("field must be str or a sequence of str")

    def get(self, field: str, fallback: Any = None) -> Any:
        value = self.database.get(self._get_field_key(field))
        return self._convert(field, value) if value is not None else fallback

    def __setitem__(self, field: str, value: Any) -> None:
        self.database.set(self._get_field_key(field), value)
        self._lazy_fields.add(field)

    def __delitem__(self, field: str) -> None:
        self.database.delete(self._get_field_key(field))
        self._lazy_fields.remove(field)

    def __contains__(self, field: str) -> bool:
        return field in self._lazy_fields

    def __len__(self) -> int:
        return len(self._lazy_fields)

    def __iter__(self) -> Iterator[str]:
        return cast(Iterator[str], self._lazy_fields)

    def keys(self) -> List[str]:
        return cast(List[str], self._lazy_fields.members())

    def values(self) -> List[Any]:
        return self.__getitem__(self.keys())

    def items(self) -> List[Tuple[str, Any]]:
        fields = self._field_types.keys()
        values = self.database.mget([self._get_field_key(field) for field in fields])
        return [
            (field, self._convert(field, value)) for field, value in zip(fields, values)
        ]

    def setnx(self, field: str, value: Any) -> bool:
        """
        Set ``field`` to ``value`` if ``field`` does not exist.

        :returns: True if successfully set or False if the field already existed.
        """
        success = bool(self.database.setnx(self._get_field_key(field), value))
        self._lazy_fields.add(field)
        return success

    @chainable_method
    def update(self, __data: Optional[Dict[str, Any]] = None, **kwargs: Any) -> bool:
        if __data is None:
            __data = kwargs
        else:
            __data.update(kwargs)
        success = self.database.mset(
            {self._get_field_key(field): value for field, value in __data.items()}
        )
        self._lazy_fields.add(*__data.keys())
        return cast(bool, success)

    def incr(self, field: str, incr_by: int = 1) -> int:
        value = self.database.incrby(self._get_field_key(field), incr_by)
        self._lazy_fields.add(field)
        return cast(int, value)

    def incr_float(self, field: str, incr_by: float = 1.0) -> float:
        value = self.database.incrbyfloat(self._get_field_key(field), incr_by)
        self._lazy_fields.add(field)
        return cast(float, value)

    def delete(self):
        self.database.delete(*[self._lazy_fields.key, *self.keys()])


class Arm(BaseModel):
    _hash: TypedHash = PrivateAttr()

    id: str

    def __init__(self, db: Database, key: str, **data: Any) -> None:
        super().__init__(id=key.split(":")[-1], **data)

        self._hash = TypedHash(
            db, key, {key: field.type_ for key, field in self.__fields__.items()}
        )

        __dict__ = object.__getattribute__(self, "__dict__")
        new_keys = set(__dict__.keys()).difference(self._hash.keys())
        for key in new_keys:
            self._hash[key] = __dict__[key]

    def __getattribute__(self, name: str) -> Any:
        if name == "__dict__":
            return self._hash
        if name == "id" or name.startswith("_"):
            return super().__getattribute__(name)
        if name in self.__fields__:
            raise AttributeError
        return super().__getattribute__(name)

    def __getattr__(self, name: str) -> Any:
        return self._hash[name]

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__fields__:
            self._hash[name] = value
        else:
            super().__setattr__(name, value)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Arm) and self.id == other.id

    def incr(self, name: str, incr_by: Union[float, int] = 1) -> Union[float, int]:
        if isinstance(incr_by, int):
            return self._hash.incr(name, incr_by)
        else:
            return self._hash.incr_float(name, incr_by)

    def lock(self, ttl: int = 100) -> Lock:
        return Lock(self._hash.database, self._hash.key, ttl)

    def delete(self):
        self._hash.delete()


A = TypeVar("A", bound=Arm)


class Bandit(Generic[A]):
    def __init__(self, redis_url: str, prefix: str, arm_type: Type[A]) -> None:
        super().__init__()

        self._redis_url = redis_url
        self._prefix = prefix
        self._arm_type = arm_type

    @property
    def db(self) -> Database:
        if not hasattr(self, "_lazy_db"):
            self._lazy_db = Database.from_url(self._redis_url, decode_responses=True)
        return self._lazy_db

    @property
    def _lazy_arm_ids(self) -> WalrusSet:
        if not hasattr(self, "__lazy_arm_ids"):
            self.__lazy_arm_ids = self.db.Set(f"{self._prefix}:arm_ids")
        return self.__lazy_arm_ids

    @property
    def arm_ids(self) -> Set[str]:
        return cast(Set[str], self._lazy_arm_ids.members())

    def _get_arm_key(self, id: str) -> str:
        return f"{self._prefix}:arm:{id}"

    def _construct_arm(self, id: str, **data: Any) -> A:
        return self._arm_type(self.db, self._get_arm_key(id), **data)

    @property
    def arms(self) -> Set[A]:
        return {self._construct_arm(id) for id in self.arm_ids}

    def lock(self, ttl: int = 100) -> Lock:
        return Lock(self.db, self._prefix, ttl)

    def add_arm(self, id: str, **data: Any) -> A:
        with self.lock():
            if id not in self._lazy_arm_ids:
                arm = self._construct_arm(
                    id, **data
                )  # Make sure the arm is initialized before adding to arm_ids
                self._lazy_arm_ids.add(id)
                return arm
            return self._construct_arm(id, **data)

    def delete_arm(self, id: str) -> None:
        with self.lock():
            self._lazy_arm_ids.remove(id)
            self._construct_arm(id).delete()

    # def get_field_from_arms(self, arm_ids: List[str], field: str) -> List[Any]:
    #     assert field in self._arm_type.__fields__
    #     pipe = self.db.pipeline()
    #     for arm_id in arm_ids:
    #         pipe.hget(self._get_arm_key(arm_id), field)
    #     return [
    #         self._arm_type.__fields__[field].type_(result) for result in pipe.execute()
    #     ]

    def get_field_from_arms(self, arm_ids: List[str], field: str) -> List[Any]:
        assert field in self._arm_type.__fields__

        keys = [f"{self._get_arm_key(arm_id)}:{field}" for arm_id in arm_ids]

        return [
            self._arm_type.__fields__[field].type_(value)
            for value in self.db.mget(keys)
        ]

    def __getitem__(self, id: str) -> A:
        if id not in self._lazy_arm_ids:
            raise KeyError(id)
        return self._construct_arm(id)

    def __len__(self):
        return len(self._lazy_arm_ids)

    def __getstate__(self):
        state = self.__dict__.copy()
        for key in self.__dict__.keys():
            if "_lazy" in key:
                del state[key]
        return state
