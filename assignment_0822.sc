trait Person {
  val name: String
  val age: Int
  var x: Int = 0
  var y: Int = 0
  val speed: Int

  def walk(destX: Int, destY: Int): Unit = {
    x = destX
    y = destY
    println(s"$name 이/가 ($x, $y)로 걷습니다.")
  }
}

class GrandParent(val name: String, val age: Int) extends Person {
  val speed: Int = 1
}

class Parent( val name: String, val age: Int) extends Person {
  val speed: Int = 3

  def run(destX: Int, destY: Int): Unit = {
    x = destX
    y = destY
    println(s"$name 이/가 ${speed + 2} 속도로 ($x, $y)로 뛰어갑니다.")
  }
}

class Child(val name: String, val age: Int) extends Person {
  val speed: Int = 5

  def run(destX: Int, destY: Int): Unit = {
    x = destX
    y = destY
    println(s"$name 이/가 ${speed + 2} 속도로 ($x, $y)로 뛰어갑니다.")
  }

  def swim(destX: Int, destY: Int): Unit = {
    x = destX
    y = destY
    println(s"$name 이/가 ${speed + 1} 속도로 ($x, $y)로 수영해서 갑니다.")
  }
}

object Main extends App {
  val grandParent: Person = new GrandParent("조부모", 70)
  val parent: Person = new Parent("부모", 40)
  val child: Person = new Child("자식", 10)

  val people = List(grandParent, parent, child)

  people.foreach(it => {
    println(s"${it.name}, ${it.age}세, 속도: ${it.speed}, 위치: (${it.x}, ${it.y})")
    it.walk(1, 1)
    it match {
      case person: Child =>
        person.run(2, 2)
        person.swim(3, -1)
      case person: Parent =>
        person.run(2, 2)
      case _ =>
    }
    println()
  })
}
