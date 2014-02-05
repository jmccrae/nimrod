package nimrod.mt

import java.util.regex.Pattern

trait Tokenizer {
  def tokenize(string : String) : Seq[String]
}

class StandardRegexTokenizer extends Tokenizer {
  val p1 =
    Pattern.compile("(\\.\\.\\.+|[\\p{Po}\\p{Ps}\\p{Pe}\\p{Pi}\\p{Pf}\u2013\u2014\u2015&&[^'\\.]]|(?<!(\\.|\\.\\p{L}))\\.(?=[\\p{Z}\\p{Pf}\\p{Pe}]|\\Z)|(?<!\\p{L})'(?!\\p{L}))")
  val p2 = Pattern.compile("\\p{C}|^\\p{Z}+|\\p{Z}+$")
  val p3 = Pattern.compile("\\p{Z}+")


  def tokenize(string : String) = p3.split(p2.matcher(p1.matcher(string).replaceAll(" $1 ")).replaceAll(""))
}
