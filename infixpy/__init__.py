"""
infixpy - Infix data structures for Python

"""

# Copyright 2019 Matt Hagy <matthew.hagy@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the “Software”), to deal in
# the Software without restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
# Software, and to permit persons to whom the Software is furnished to do so, subject
# to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


import operator
import copy
from functools import reduce
from itertools import chain
from collections import defaultdict, Counter, deque
from typing import Callable, Any, Union, Iterable
import numpy as np

__all__ = ['Seq', 'IList', 'IFrozenList', ]

CallableTypes = Union[Callable, str, int]
NoneType = type(None)


def identity(x):
    return x


def get_callable(obj: CallableTypes) -> Callable[[Any], Any]:
    if callable(obj):
        return obj
    elif isinstance(obj, str):
        return operator.attrgetter(obj)
    elif isinstance(obj, int):
        return operator.itemgetter(obj)
    else:
        raise TypeError("Can't call %s" %obj)

class StrMixin:
    def __init__(self):
        pass

    @staticmethod
    def listrepr(lst):
        from typing import Iterable
        if not lst:
            return ''
        elif isinstance(lst,str) or not isinstance(lst, Iterable):
            return repr(lst)
        else:
            return StrMixin.mkstring(lst, ',','[',']')

    @staticmethod
    def mkstring(lst, between, before='',after=''):
        if not lst:
            ret = ''
        elif isinstance(lst,str) or not isinstance(lst, Iterable):
            ret = lst
        else:
            ret = '%s%s%s' %(before,
                             between.join([StrMixin.listrepr(x) for x in lst]),
                             after)
        return ret
                                             
class IterableMixin(StrMixin):
    """Adds some useful Scala-inspired methods for working with iterables"""
    
    def tolist(self) -> 'IList':
        return list(self)

    def tonumpy(self) -> 'IList':
        return np.array(list(self))

    def tofrozenlist(self) -> 'IFrozenList':
        return IFrozenList(self)

    def todict(self) -> 'IDict':
        return IDict(self)

    def map(self, func_like: CallableTypes) -> 'Seq':
        return Seq(map(get_callable(func_like), self))

    def flatmap(self, func_like: CallableTypes) -> 'Seq':
        func = get_callable(func_like)
        return Seq(y for x in self for y in func(x))

    def foreach(self, func_like: CallableTypes):
        func = get_callable(func_like)
        for x in self:
            func(x)

    def take(self, n) -> 'Seq':
        return Seq(x for _, x in zip(range(n), self))

    def drop(self, n) -> 'Seq':
        def gen():
            i = iter(self)
            try:
                for _ in range(n):
                    next(i)
            except StopIteration:
                pass
            yield from i

        return Seq(gen())

    def last(self, n) -> 'Seq':
        def gen():
            d = deque()
            for x in self:
                d.append(x)
                if len(d) > n:
                    d.popleft()
            yield from d

        return Seq(gen())

    def filter(self, func_like: CallableTypes) -> 'Seq':
        return Seq(filter(get_callable(func_like), self))

    def chain(self, other) -> 'Seq':
        return Seq(chain(self, other))

    def apply(self, func):
        return get_callable(func)(self)

    def applyseq(self, func) -> 'Seq':
        return Seq(self.apply(func))

    def fold(self, init_value: Any, func_like: CallableTypes):
        return reduce(get_callable(func_like), self, init_value)

    def reduce(self, func_like: CallableTypes):
        return reduce(func_like, self)

    def sum(self):
        return sum(self)

    def count(self) -> int:
        return sum(1 for _ in self)

    def valuecounts(self) -> 'IDict':
        return IDict(Counter(self))

    def sortby(self, rank_func_like: CallableTypes) -> 'IList':
        return IList(sorted(self, key=get_callable(rank_func_like)))

    def sort(self) -> 'IList':
        return self.sortby(identity)

    def distinct(self) -> 'IList':
        return IList(set(self))

    def groupby(self, key_func_like: CallableTypes) -> 'IDict':
        key_func_like = get_callable(key_func_like)
        d = defaultdict(IList)
        for el in self:
            d[key_func_like(el)].append(el)
        return IDict(d)

    def keyby(self, key_func_like: CallableTypes) -> 'IDict':
        key_func_like = get_callable(key_func_like)
        d = IDict()
        for el in self:
            k = key_func_like(el)
            if k in d:
                raise ValueError(f"duplicate key {k!r}")
            d[k] = el
        return d

    def aggregateby(self,
                    key: CallableTypes,
                    create_aggregate: CallableTypes,
                    add_toaggregate: CallableTypes) -> 'IDict':
        key = get_callable(key)
        create_aggregate = get_callable(create_aggregate)
        add_toaggregate = get_callable(add_toaggregate)

        aggs_by_key = {}
        for x in self:
            k = key(x)
            try:
                agg = aggs_by_key[k]
            except KeyError:
                agg = aggs_by_key[k] = create_aggregate(x)
            aggs_by_key[x] = add_toaggregate(agg, x)

        return IDict(aggs_by_key)

    def foldby(self,
               key: CallableTypes,
               agg0,
               add_toaggregate: CallableTypes) -> 'IDict':
        return self.aggregateby(key, lambda _: agg0, add_toaggregate)

    def reduceby(self, key: CallableTypes, reducer: CallableTypes) -> 'IDict':
        key = get_callable(key)
        reducer = get_callable(reducer)

        existings_by_key = {}
        for x in self:
            k = key(x)
            try:
                existing = existings_by_key[k]
            except KeyError:
                existings_by_key[k] = x
            else:
                existings_by_key[k] = reducer(existing, x)

        return IDict(existings_by_key)

    def enumerate(self):
        return Seq(enumerate(self))

    def mkstring(self, between, before='',after=''):
        return super(StrMixin).mkstring(self,between,before,after)

    def listrepr(self):
        return super().listrepr(self)

class Seq(IterableMixin):
    """
    Wrapper around arbitrary sequences. Assumes sequences are single pass and can't be iterated
    over multiple times. Use `IList` for realized sequences that can be iterated over multiple
    times.
    """

    def __init__(self, seq: Iterable):
        self._seq = seq
        self._ran = False

    def tee(self, foreachfunc):
        tmpseq = list(iter(self._seq))
        for x in iter(tmpseq):
            foreachfunc(x)
        self._seq = tmpseq.copy()
        return self
    
    def __iter__(self):
        if self._ran:
            print("Warning: re-running sequence")
        self._ran = True
        try:
            return iter(self._seq)
        finally:
            # remove the reference so that we can free up sources in GCing
            # while realizing the rest of the sequence of operations
            # pass
            del self._seq

    def __repr__(self):
        return f'{self.__class__.__name__}({self._seq!r})'

    def mkstring(self, between, before='',after=''):
        return super(IterableMixin,self).mkstring(self._seq,between,before,after)

    def listrepr(self):
        return super(IterableMixin,self).listrepr(self._seq)


class ListMixin(StrMixin):
    def __init__(self, l=()):
        self._list = l

    @property
    def length(self):
        return len(self._list)

    def reverse(self) -> Seq:
        return Seq(reversed(self._list))

    def __iter__(self):
        return iter(self._list)

    def __repr__(self):
        return self.listrepr(self._list)

    def mkstring(self, between, before='',after=''):
        return super(StrMixin).mkstring(self._list,between,before,after)

    def listrepr(self):
        return super(StrMixin).listrepr(self._list)


class IList(IterableMixin, ListMixin):
    """
    Wrapper around Python lists with a more Scala-esque API

    TODO: Add more modification methods beyond append
    """

    def __init__(self, l=()):
        super().__init__(list(l) if not isinstance(l, list) else l)

    def tolist(self):
        return self

    def append(self, x):
        self._list.append(x)
      
    def copy(self):
        return IList(list(self._l))

    def __copy__(self):
        return self.copy()

    def __deepcopy__(self, memo=None):
        return IList(copy.deepcopy(x, memo=memo) for x in self._l)

    def mkstring(self, between, before='',after=''):
        return StrMixin.mkstring(self._list,between,before,after)

    def listrepr(self):
        return StrMixin.listrepr(self._list)


class IFrozenList(IterableMixin, ListMixin):
    """
    Immutable wrapper around Python lists
    """

    def __init__(self, l):
        if not isinstance(l, tuple):
            l = tuple(l)
        super().__init__(l)

    def tofrozenlist(self):
        return self


class IDict(dict):
    """
    Extension of Python dict
    """

    @property
    def length(self):
        return len(self)

    def keys(self):
        return Seq(super().keys())

    def values(self):
        return Seq(super().values())

    def items(self):
        return Seq(super().items())

    def mapvalues(self, func_like: CallableTypes) -> 'IDict':
        func = get_callable(func_like)
        return IDict({k: func(v) for k, v in self.items()})

    def union(self, other: Union['IDict', dict], error_on_overlap=False):
        if error_on_overlap:
            common_keys = set(self.keys()) & set(other.keys())
            if common_keys:
                raise ValueError(f"there are {len(common_keys)} in common when non were expected")

        cp = IDict(self)
        cp.update(other)
        return cp

    def join(self, other: 'IDict', how='inner'):
        if how == 'inner':
            keys = set(self.keys()) & set(other.keys())
        elif how == 'outer':
            keys = set(self.keys()) | set(other.keys())
        elif how == 'left':
            keys = self.keys()
        elif how == 'right':
            keys = other.keys()
        else:
            raise ValueError(f"Invalid join {how!r}. Must be either inner, outer, left or right")

        def gen():
            for key in keys:
                yield key, (self.get(key), other.get(key))

        return Seq(gen())
