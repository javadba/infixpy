"""
Example of reconstructing a lazy sequence so that we can realize it multiple times
"""

from scalaps import Seq


def create_seq():
    return (Seq(range(1000))
            .map(lambda x: x * 10)
            .filter(lambda x: x % 70 == 0)
            .flatmap(lambda x: range(x)))


create_seq().take(3).foreach(print)
# > 0
# > 1
# > 2

create_seq().last(3).foreach(print)
# > 9937
# > 9938
# > 9939

print('count', create_seq().count())
# > count 710710

print('sum', create_seq().sum())
# > sum 2362755395
