val day = "thu"
val offers = day.toLowerCase() match {
  case "mon" | "tue" => "40%"
  case "wed" | "thu" => "30%"
  case "sun" => "no offers"
}


val total = 12345
val off = total match{
  case x if (x> 1000 && x<=   5000) => s"10% off you saved $(total*10/100) rupees"
    case x if (x> 5000 && x<=  10000 )=> s"20% off you saved $(total*20/100) rupees"
    case x if (x> 50000 ) => s"40% off you saved $(total*20/100) rupees"
    case _ => "no offers"
    }

/*val names = Array("venu","naveen","praveen","modi")
// for loop

val res = for(x<- names) println(x+"testing")*/
