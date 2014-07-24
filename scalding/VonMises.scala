/*
Algorithm Details: See http://en.wikipedia.org/wiki/Power_iteration
To run: $ scald.rb --hdfs-local VonMises.scala
*/

import com.twitter.scalding._

class VonMises(args:Args) extends Job(args) {

  type Scalar = TypedPipe[Double]

  /*
  Consider a vector like [43,21,56,78]

  That is stored in a 4 by 2 pipe like so
  0 43
  1 21
  2 56
  3 78
  the first column contains the index of the cell
  the second column contains the value of the cell
  */
  type Vector = TypedPipe[(Int, Double)]

  /*
  Consider a matrix like
  4  2  5
  2  6  8
  1  5  2

  That is stored in a 3 by 2 pipe like so
  0 [4,2,5]
  1 [2,6,8]
  2 [1,5,2]

  [index, row] ie. 2 columns
  the first column contains the index of the row
  the second column contains an entire row of the matrix
  */
  type Matrix = TypedPipe[(Int, Seq[Double])]

  // Linear transformation aka Multiply matrix with vector
  def mult(matrix:Matrix, vector:Vector): Vector = {
    matrix.cross(vector)
    .map {
      rowMatrixCellVector =>
      val (rowMatrix, cellVector) = rowMatrixCellVector
      val (rowIndex, row) = rowMatrix
      val (cellIndex, cell) = cellVector
      (rowIndex, row(cellIndex)*cell)
    }
    .group
    .sum
    .toTypedPipe
  }

  // l2 norm aka euclidean norm of vector
  def l2(v:Vector): Scalar = {
    v.map{ indexValue =>
      val (index, value) = indexValue
      value*value
    }.groupAll
     .sum
     .values
     .map {
      x => math.sqrt(x)
    }
  }


  // divide vector by scalar aka Scaling
  def div(v:Vector, c:Scalar): Vector = {
    v.cross(c)
    .map { indexValuescale =>
      val (indexValue, scale) = indexValuescale
      val (index, value) = indexValue
      (index, value/scale)
    }
  }

  def vonMisesStep(guessVector:Vector, matrix:Matrix): (Scalar, Vector) = {
    val result = mult(matrix,guessVector)
    val eigen = l2(result)
    (eigen, div(result, eigen)) // aka normalize matrix
  }

  // computes the dominant eigen value and associated eigen vector via Von Mises Iteration aka Power Method
  def vonMises(matrix:Matrix): Scalar = {

    val guessEigen = TypedPipe.from(Seq(1.0))
    val guessVector = {
      matrix.map{
        indexRow =>
        val (index, row) = indexRow
        (index, 1.0)
      }
    }

    /* Von Mises Iteration
     Given any matrix A & an eigenvector x => Repeatedly normalize A*x until x converges! (atmost 5 times)
     To bootstrap, just pick any random nonzero eigenvector - we use [1,1,1,...] in this example
     Each step of the normalization returns a new eigenvalue & the associated eigenvector
    */
    val (trueEigen, trueEigenVector) = (0 until 5).foldLeft ((guessEigen, guessVector)) { (a,b) =>
      val (guessEigen, guessVector) = a
      vonMisesStep(guessVector, matrix)
    }

    trueEigen
  }

/* A 4x4 symmetric matrix
7 1 2 4
1 8 3 2
2 3 9 1
4 2 1 11

This is stuffed into a pipe like so -
[index, row] ie. 2 columns
the first column contains the index of the row
the second column contains an entire row of the matrix
*/
  val symmetric = {
    Seq(Seq(7.0,1,2,4), Seq(1,8.0,3,2), Seq(2,3,9.0,1), Seq(4,2,1,11.0))
    .zipWithIndex
    .map {  rowIndex =>
      val (row, index) = rowIndex
      (index, row)
    }
  }

  vonMises(TypedPipe.from(symmetric)).write(TypedTsv[Double]("trueEigen"))

  // now try a very large matrix
  val large = {
    Seq.tabulate[Seq[Double]](1000)( row =>
      Seq.tabulate[Double](1000)(column => if (row==column) 100+ math.random*100 else math.random*5))
    .zipWithIndex
    .map {  rowIndex =>
      val (row, index) = rowIndex
      (index, row)
    }
  }
  vonMises(TypedPipe.from(large)).write(TypedTsv[Double]("trueEigenLarge"))


}
