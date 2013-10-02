import org.mapdb.DB
import java.io.FileInputStream
import java.util.zip.GZIPInputStream

val ptIn = opts.roFile("phrase-table","The phrase table to merge")
val lexe2f = opts.roFile("lex.e2f","The lexical weights e2f")
val lexf2e = opts.roFile("lex.f2e","The lexical weights f2e")
val outFile = opts.woFile("out","Where to write the new phrase table to")
opts.restAsSystemProperties
opts.verify

val cacheSize = System.getProperty("cacheSize","1048576").toInt

namedTask("Merge PT") {
  val db = org.mapdb.DBMaker.newTempFileDB().cacheSize(cacheSize).make() 

  var lastForeign = ""
  var lastTranslation = ""
  var lastAlignment = ""
  var bothTotal = 0
  var firstTotal = 0
  var secondTotal = 0

  val lexicalline = "(.* .*) (.*)".r

  def readLexicalProb(file : File, db : DB) = {
    var lexicalProb = db.getTreeMap[String,Double](file.getName())
      //collection.mutable.Map[String,Double]()
    val lexicalIn = file.getName() endsWith ".gz" match {
      case true => io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(file)))
      case false => io.Source.fromFile(file)
    }

    for(line <- lexicalIn.getLines) {
      line match {
        case lexicalline(key,p) => lexicalProb.put(key, p.toDouble)
        case _ => System.err.println("bad line: " + line)
      }
    }
    lexicalProb
  }

  System.err.println("Read e2f lex")
  val e2fProb = readLexicalProb(lexe2f,db)
  System.err.println("Read f2e lex")
  val f2eProb = readLexicalProb(lexf2e,db)

  def calculateProb(count : Int, both : Int) : String = {
    (count.toDouble / both.toDouble).toString
  }

  val alignElement = "(\\d+)-(\\d+)".r

  class Alignment(val a : Int, val b : Int) extends Ordered[Alignment] {
    def this(a : String, b : String) = this(a.toInt,b.toInt)

    def compare(that : Alignment) = if(this.a - that.a == 0) {
      this.b - that.b
    } else {
      this.a - that.a
    }
  }

  def calculateLex(foreign : String, translation : String, alignment : String, alignDir : Boolean) = {
    val ftks = foreign split "\\s+"
    val ttks = translation split "\\s+"
    val as = (alignment split "\\s+" map ({ 
      case alignElement(a2,a1) => if(alignDir) {
        new Alignment(a1,a2)
      } else {
        new Alignment(a2,a1)
      }
    })).toList.sorted

    var score = 1.0

    for(i <- 0 until ttks.length) {
      val a2 = as.filter(_.a == i)
      val n = a2.size    
      var s = 0.0
      if(n > 0) {
        for(j <- a2.map(_.b)) {      
          val key = ttks(i) + " " + ftks(j)
          if(alignDir) {
            Option(f2eProb.get(key)) match {
              case Some(p) => s += p / n
              case None => // noop
            }
          } else {
            Option(e2fProb.get(key)) match {
              case Some(p) => s += p / n
              case None => // noop
            }
          }
        }
      } else {
        val key = ttks(i) + " NULL"
        if(alignDir) {
          Option(f2eProb.get(key)) match {
            case Some(p) => s += p
            case None => // noop
          }
        } else {
          Option(e2fProb.get(key)) match {
            case Some(p) => s += p
            case None => // noop
          }
        }
      }
      if(s != 0.0) {
        score *= s
      }
    }
    if(score.isInfinite || score.isNaN) {
      "1e-8"
    } else {
      score.toString
    }
  }

  val mosesline = "(.*) \\|\\|\\| (.*) \\|\\|\\| (.*) \\|\\|\\| (.*) \\|\\|\\| (\\d+) (\\d+) (\\d+)".r

  var linesRead = 0

  System.err.println("Read PT")

  val out = opts.openOutput(outFile)

  for(line <- io.Source.fromFile(ptIn).getLines) {
    line match {
      case mosesline(foreign,translation,scores,alignment,first,second,both) => {
        if(foreign == lastForeign && translation == lastTranslation) {
          bothTotal += both.toInt
          firstTotal += first.toInt
          secondTotal += second.toInt
        } else {
          if(lastForeign != "") {
            out.println(lastForeign + " ||| " + lastTranslation + " ||| " + calculateProb(bothTotal,firstTotal) + " " +
              calculateLex(lastTranslation,lastForeign,lastAlignment,false) + " " +
              calculateProb(bothTotal,secondTotal) + " " +
              calculateLex(lastForeign,lastTranslation,lastAlignment,true) + " 2.718 ||| " + lastAlignment +
              " ||| " + firstTotal + " " + secondTotal + " " + bothTotal)         
          }
          bothTotal = both.toInt
          firstTotal = first.toInt
          secondTotal = second.toInt
          lastForeign = foreign
          lastTranslation = translation
          lastAlignment = alignment
        }
      }
      case _ => System.err.println("bad line: " + line)
    }
    linesRead += 1
    if(linesRead % 1000000 == 0) {
      System.err.print(".")
    }
  }
  System.err.println()
  out.flush()
  out.close()

  db.close()
}
