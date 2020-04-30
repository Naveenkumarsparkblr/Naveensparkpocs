val age = 20
val res = {
if(age>0 && age<10) "kid"
else if (age>=10 && age<=18) "minor"
else if (age>18 && age<60) "major"
else if (age>=60 && age<100) "old aged"
else "pls check ur input"
}

def test(age:Int) = {
  if(age>0 && age<10) "kid"
  else if (age>=10 && age<=18) "minor"
  else if (age>18 && age<60) "major"
  else if (age>=60 && age<100) "old aged"
  else "pls check ur input"
}
