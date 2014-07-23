object VonMises extends App {
  type Matrix = Seq[Seq[Double]]
  type Vector = Seq[Double]

  def mkMatrix(rows:Int) = (0 until rows).map{ r => mkDiagVector(rows, r) }
  def mkDiagVector(rows:Int, column:Int) = List.tabulate[Double](rows)(i => if (i==column) 10 + math.random * 100 else math.random)

  def dot(a:Vector, b:Vector) = a.zip(b).map(i=>i._1 * i._2).sum // dot product aka inner product
  def mult(m:Matrix, v:Vector) = m.map{ row => dot(row,v) } // matrix vector multiplication
  def l2(v:Vector) = math.sqrt(v.map{i=> i*i}.sum) // l2 norm of vector
  def scale(v:Vector, c:Double) = v.map{i=> i*c }

  // computes the dominant eigen value and associated eigen vector via Von Mises Iteration aka Power Method
  def vonMises(m:Matrix): (Double, Vector) = {
    val TOL = math.pow(10.0,-4) // error tolerance bound
    val (guessEigen, guessVector) = (1.0, Seq(1,1,1,1.0))

    def vonMisesStep(guess:Vector, m:Matrix): (Double, Vector) = {
      val result = mult(m,guess)
      val eigen = l2(result)
      (eigen, scale(result, 1.0/eigen)) // aka normalize matrix
    }

    // Von Mises Iteration, given matrix A & vector x = Simply perform A*x/||A*x||, 100 times or until convergence!
    val (trueEigen, trueEigenVector, mustCompute) = (0 to 100).foldLeft ((guessEigen, guessVector, true)) { (a,b) =>
      val (guessEigen, guessVector, mustCompute) = a
      if (mustCompute) {
        val (eigen, eigenVector) = vonMisesStep(guessVector, m)
        //println(eigen + "," + eigenVector)
        (eigen, eigenVector, math.abs(eigen - guessEigen) > TOL)
      } else a
    }

    (trueEigen, trueEigenVector)
  }

  // Goal : To compute dominant eigenvalue of symmetric matrix below
  val symmetric = Seq(Seq(7.0,1,2,4), Seq(1,8.0,3,2), Seq(2,3,9.0,1), Seq(4,2,1,11.0))
  val (dominantEigen, dominantEigenVector) = vonMises(symmetric)
  println(dominantEigen + "," + dominantEigenVector)

  /* We OOM around 1500 rows. So the largest matrix we can handle is about 1400x1400
  List.tabulate[Int](100)(i=> 1000+ 100*i).map{ i=>
    val m = mkMatrix(i)
    val (dominantEigen, dominantEigenVector) = vonMises(m)
    println(i + ":")
    println(m)
    println(dominantEigen)// + "," + dominantEigenVector)
  }
  */
}
