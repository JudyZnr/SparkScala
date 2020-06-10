var list = 1.to(5)
list foreach println

var x1:Int = 0
var x2:Int = 1

for (i <- 0 to 10){
  val res: Int = x1 + x2
  println(res)
  x1 = x2
  x2 = res
}

def ConvertUpper (old:String ,f:String => String) : String = {
  f(old)
}
ConvertUpper("hey", old => old.toUpperCase)

