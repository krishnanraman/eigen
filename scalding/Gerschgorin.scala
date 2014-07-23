/*
Algorithm Details: https://gist.github.com/krishnanraman/c17397667ea9090a5bc1
To run: $ scald.rb --hdfs-local Cassini.scala
*/
import com.twitter.scalding._

class Gerschgorin(args:Args) extends Job(args) {

  /* this is a symmetric matrix
  7 1 2
  1 8 3
  2 3 9
  */
  val symmetric = Seq(Seq(7,1,2.0), Seq(1,8,3.0), Seq(2,3,9.0))

  // now its sitting in a Scalding Typed Pipe
  // a Pipe is a "description of a computation" ( Posco)
  // that means you can compute eigen values & suchlike from this pipe.
  val pipe = TypedPipe.from(symmetric.zipWithIndex)

  // lets first gather the cells along the diagonal
  val diagonals = pipe.map { rowIndex => val (row, index) = rowIndex; (index, row(index)) }
  //diagonals: com.twitter.scalding.typed.TypedPipe[(Int, Double)] = IterablePipe(List((0,7.0), (1,8.0), (2,9.0)),cascading.flow.FlowDef@6d639189,Local(false))

  // now lets scoop up the non diagonal entries
  val nonDiagonals = pipe.map { rowIndex => val (row, index) = rowIndex; (index, (0 until index).map{row} ++ (index+1 until row.size).map{row})}
  //nonDiagonals: com.twitter.scalding.typed.TypedPipe[(Int, scala.collection.immutable.IndexedSeq[Double])] = IterablePipe(List((0,Vector(1.0, 2.0)), (1,Vector(1.0, 3.0)), (2,Vector(2.0, 3.0))),cascading.flow.FlowDef@6d639189,Local(false))

  // lets take the non-diagonals & sum them per row, after getting the norm of each entry
  // ( norm of a scalar is simply the absolute value)
  val nonDiagonalNormSum = nonDiagonals.map { indexRow => val (index, row) = indexRow; (index, row.map{math.abs}.sum)}
  //nonDiagonalNormSum: com.twitter.scalding.typed.TypedPipe[(Int, Double)] = IterablePipe(List((0,3.0), (1,4.0), (2,5.0)),cascading.flow.FlowDef@6d639189,Local(false))

  // now lets join the diagonals with their nondiagonal norms
  // that, my friends, is the gershgorin!
  val gershgorin = diagonals.join(nonDiagonalNormSum).values
  /*
  gershgorin: com.twitter.scalding.typed.TypedPipe[(Double, Double)] = TypedPipeInst(Each(_pipe_10*_pipe_11)[Identity[decl:'key', 'value']],'key', 'value',<function1>)

  // if you want to see what that looks like, dump it
  scala> gershgorin.dump
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell] starting
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.32696373550075297"]
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.5669129290001957"]
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.40624991664526333"]
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
  14/07/18 15:15:15 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
  14/07/18 15:15:15 INFO flow.FlowStep: [ScaldingShell] starting step: local
  (7.0,3.0)
  (8.0,4.0)
  (9.0,5.0)

  // So the first gershgorin circle sits at 7, with a radius of 3 ie. it goes from [4, 10] along the real line
  // So the second gershgorin circle sits at 8, with a radius of 4 ie. it goes from [4, 12] along the real line
  // So the third gershgorin circle sits at 9, with a radius of 5 ie. it goes from [4, 14] along the real line
  // these are precisely the bounds of the eigen values as well !
  */

  val eigenValues = gershgorin.map { centerRadius => val (center, radius) = centerRadius; (center - radius, center + radius)}
  /*eigenValues: com.twitter.scalding.typed.TypedPipe[(Double, Double)] = TypedPipeInst(Each(_pipe_10*_pipe_11)[Identity[decl:'key', 'value']],'key', 'value',<function1>)

  scala> eigenValues.dump
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell] starting
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.32696373550075297"]
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.5669129290001957"]
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.37809438925390637"]
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
  14/07/18 14:55:08 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
  14/07/18 14:55:08 INFO flow.FlowStep: [ScaldingShell] starting step: local
  (4.0,10.0)
  (4.0,12.0)
  (4.0,14.0)
  */
  // now that we have the eigens, just sort them & take the max
  // the biggest eigen is the spectral radius
  val spectralRadius = eigenValues.map { minmax => val (min,max) = minmax; max }.groupAll.sorted.reverse.head.values
  eigenValues.write(TypedTsv("eigenvalues-gershgorin"))

  /*spectralRadius: com.twitter.scalding.typed.TypedPipe[Double] = TypedPipeInst(Every(_pipe_10*_pipe_11)[TypedBufferOp[decl:'value']],'key', 'value',<function1>)

  scala> spectralRadius.dump
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell] starting
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.32696373550075297"]
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell]  source: MemoryTap["NullScheme"]["0.5669129290001957"]
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell]  sink: MemoryTap["NullScheme"]["0.606555052652465"]
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell]  parallel execution is enabled: true
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell]  starting jobs: 1
  14/07/18 14:59:52 INFO flow.Flow: [ScaldingShell]  allocating threads: 1
  14/07/18 14:59:52 INFO flow.FlowStep: [ScaldingShell] starting step: local
  14.0

  // Voila! That is the L2 Norm of this Symmetric Matrix.
  // Lets now confirm our intiutions using Wolfram's $$$$ WolframAlpha
  From WolframAlpha, the eigens are:
   {12.4188, 6.38677, 5.1944}

   Notice that
   12.4 lies between [4,14]
   6.4 lies between [4,12]
   5.2 lies between [4,10]
  and true spectral radius = max eigen = 12.4 is well approximated by 14
  */
}
