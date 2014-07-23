/*
Algorithm Details: https://gist.github.com/krishnanraman/c17397667ea9090a5bc1
To run: $ scald.rb --hdfs-local Cassini.scala
*/
import com.twitter.scalding._

class Cassini(args:Args) extends Job(args) {

/* A 4x4 symmetric matrix
7 1 2 4
1 8 3 2
2 3 9 1
4 2 1 11
*/
  val symmetric = Seq(Seq(7,1,2,4), Seq(1,8,3,2), Seq(2,3,9,1), Seq(4,2,1,11))
  val pipe = TypedPipe.from(symmetric.zipWithIndex)

  val diagonals = pipe.map { rowIndex =>
    val (row, index) = rowIndex
    (index, row(index))
  }

  val nonDiagonals = pipe.map { rowIndex =>
    val (row, index) = rowIndex
    (index, (0 until index).map{row} ++ (index+1 until row.size).map{row})
  }

  val nonDiagonalNormSum = nonDiagonals.map{ idxRow => val (idx, row) = idxRow; (idx, row.sum) }
  /*
  nonDiagonalNormSum: com.twitter.scalding.typed.TypedPipe[(Int, Int)] = IterablePipe(List((0,7), (1,6), (2,6), (3,7)),cascading.flow.FlowDef@6a87832d,Local(false))
  */

  val diagNonDiagPipe = diagonals.join(nonDiagonalNormSum)

  val nC2combinations = diagNonDiagPipe.cross(diagNonDiagPipe).filter {
    ab =>
    val (a,b) = ab
    a._1 < b._1
  }

  /*
scala> nC2combinations.dump
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell] starting
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.2594611917490499"]
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.7722493355962736"]
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.747368470074863"]
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
14/07/19 22:07:22 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
14/07/19 22:07:22 INFO flow.FlowStep: [ScaldingShell] starting step: local
((0,(7,7)),(1,(8,6)))
((0,(7,7)),(2,(9,6)))
((0,(7,7)),(3,(11,7)))
((1,(8,6)),(2,(9,6)))
((1,(8,6)),(3,(11,7)))
((2,(9,6)),(3,(11,7)))
*/

  val bcPipe = nC2combinations.map { xy =>
    val (x,y) = xy
    val (aIdx,aRow) = x
    val (bIdx, bRow) = y
    val (aDiag, aRadius) = aRow
    val (bDiag, bRadius) = bRow
    val b = -(aDiag + bDiag)
    val c = aDiag*bDiag - aRadius*bRadius
    (b,c)
  }

  /*

scala> bcPipe.dump
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell] starting
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.2594611917490499"]
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.7722493355962736"]
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.14451103388434228"]
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
14/07/19 22:13:05 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
14/07/19 22:13:05 INFO flow.FlowStep: [ScaldingShell] starting step: local
(-15,14)
(-16,21)
(-18,28)
(-17,36)
(-19,46)
(-20,57)
*/

  val quadraticSolver = bcPipe.map{ bc =>
    val (b,c) = bc
    val root1 = (-b + math.sqrt(b*b-4*c))/2
    val root2 = (-b - math.sqrt(b*b-4*c))/2
    if (root1 < root2) (root1,root2) else (root2,root1)
  }

/*
scala> quadraticSolver.dump14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell] starting
14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.2594611917490499"]
14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.7722493355962736"]
14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.18617943262924586"]
14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
14/07/19 22:17:51 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
14/07/19 22:17:51 INFO flow.FlowStep: [ScaldingShell] starting step: local
(1.0,14.0)
(1.4425614756979996,14.557438524302)
(1.719890110719482,16.280109889280517)
(2.479202710603852,14.520797289396148)
(2.847932652174965,16.152067347825035)
(3.4425614756979996,16.557438524302)
*/

val min = quadraticSolver.groupAll.minBy{ x=> x._1 }.values.map{ x=> x._1}
val max = quadraticSolver.groupAll.maxBy{ x=> x._2 }.values.map{ x=> x._2}
val unionOfOvals = min.cross(max)

/*
scala> unionOfOvals.dump
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell] starting
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.2594611917490499"]
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.7722493355962736"]
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.2263624362034401"]
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
14/07/20 13:43:46 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
14/07/20 13:43:46 INFO flow.FlowStep: [ScaldingShell] starting step: local
(1.0,16.557438524302)
// All four eigens are contained in the [1,16.5] interval
*/

val largestEigen = unionOfOvals.map{ x=> x._2}
largestEigen.write(TypedTsv("largesteigen"))

/* Lets verify the same with Wolfram Alpha
eigenvalues {{7,1,2,4},{1,8,3,2},{2,3,9,1},{4,2,1,11}}
{15.5845, 9.57363, 5.9301, 3.91173}

Notice that all 4 eigen values are contained in the [1,16.5] interval
Also note that the L2 Norm of this symmetric matrix = largest eigen = 15.58
15.58 is well approximated by 16.557, the approximation obtained from the Cassini Oval.
For the record, the Gershgorin approximation for this matrix is 18.


-----
To summarize:
Consider the 4x4 symmetric matrix
7 1 2 4
1 8 3 2
2 3 9 1
4 2 1 11

True L2 Norm: 15.58
Approximate L2 Norm per Cassini Oval: 16.557
Approximate L2 Norm per Gershgorin: 18
------
*/
}
