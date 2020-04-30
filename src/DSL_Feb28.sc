//HIGHER ORDER METHODS

val num = Array(22,3,11,44,22,65,34,21)
val pr = num.map(x=>x*x).filter(x=>x<1000)
val names = Array("venu naidu","naveen naidu","hari naidu","geetanshu naidu")
names.map(x=>x.toUpperCase)

//map means apply a logic on top of each element
//in map number of input and output elements are same

//filter..apply a logic on top of each and every element based on boolean value

num.filter(x=>x<40)

names.flatten

//flatten splits each value.

names.map(x=>x.split(regex=" ")).flatten