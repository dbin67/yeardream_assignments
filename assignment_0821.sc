// 3.9.2 퀴즈 2
val wordList = List("apple", "basket", "candy")
val result = wordList.aggregate((0, 0))(
  (acc, word) => (acc._1 + 1, acc._2 + word.length),
  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)
println(result)

// 6.3.1 퀴즈 1
def fibonnaci(size: Int) : Array[Int] = {
  if (size <= 0) return Array()

  val result = Array.ofDim[Int](size)

  result(0) = 0

  if (size > 1) result(1) = 1

  for (i <- 2 until size) {
    result(i) = result(i - 1) + result(i - 2)
  }
  result
}
println(fibonnaci(5).toList)

// 6.3.2 퀴즈 2
type Row = Array[Int]
def Row(xs: Int*) = Array(xs: _*)

type Matrix = Array[Row]
def Matrix(rows: Row*) = Array(rows: _*)

// 예제 사용
implicit class MatrixOperators(_this: Matrix) {
  def *(other: Matrix): Option[Matrix] = {
    if (_this(0).length != other.length) return None
    
    val result = Array.ofDim[Int](_this.length, other(0).length)

    for (i <- _this.indices) {
      for (j <- other(0).indices) {
        result(i)(j) = (_this(0).indices).map(k => _this(i)(k) * other(k)(j)).sum
      }
    }
    Some(result)
  }
}

val mat1 = Matrix(Row(1, 2, 3), Row(4, 5, 6), Row(7, 8, 9))
val mat2 = Matrix(Row(1, 2, 3), Row(4, 5, 6), Row(7, 8, 9))

println((mat1 * mat2).toList)