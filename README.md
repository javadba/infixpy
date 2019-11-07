# infixpy 
See blog post, 
[Introducing infixpy: Scala-inspired data structures for Python](https://medium.com/@matthagy/introducing-infixpy-scala-inspired-data-structures-for-python-53f3afc8696)
to learn about using this library.
> A functional, object-oriented approach for working with sequences and collections. Also similar to Spark RDDs and Java Streams. Hope you find they simplify your code by providing a plethora of common algorithms for working with sequences and collections.

## Stephen's Direct from Scala example

Scala version

```scala
val a = ((1 to 50)
  .map(_ * 4)
  .filter( _ <= 170)
  .filter(_.toString.length == 2)
  .filter (_ % 20 == 0)
  .zipWithIndex
  .map{ case(x,n) => s"Result[$n]=$x"}
  .mkString("  .. "))

  a: String = Result[0]=20  .. Result[1]=40  .. Result[2]=60  .. Result[3]=80
```
Version using the infixpy library with python

```python
from infixpy import *
a = (Seq(range(1,51))
     .map(lambda x: x * 4)
     .filter(lambda x: x <= 170)
     .filter(lambda x: len(str(x)) == 2)
     .filter( lambda x: x % 20 ==0)
     .enumerate()                                            Ï
     .map(lambda x: 'Result[%d]=%s' %(x[0],x[1]))
     .mkstring(' .. '))
print(a)
  
  # Result[0]=20  .. Result[1]=40  .. Result[2]=60  .. Result[3]=80
```


## Original Example
```python
from infixpy import Seq

(Seq(range(10))
 .map(lambda x: x+3)
 .filter(lambda x: x%2==0)
 .group_by(lambda x: x%3)
 .items()
 .for_each(print))
```

#### Output
```Î
(1, SList([4, 10]))
(0, SList([6, 12]))
(2, SList([8]))
```

## Examples
See examples/ directory for additional examples of using infixpy. 

Also see example usages in
[career_village_entities](https://github.com/matthagy/career_village_entities).
