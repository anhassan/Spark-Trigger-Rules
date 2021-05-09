package exception

class RuleException(sourceName:String) extends RuntimeException(s"The validation for ${sourceName} failed.")
