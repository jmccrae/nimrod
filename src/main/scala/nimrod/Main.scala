package nimrod

import com.twitter.util.Eval

object Main {
  private def printUsage = {
    System.err.println("Usage: ./nimrod script.scala [args]")
    System.exit(-1)
  }

  def main(_args : Array[String]) {
    var args = _args.toList
    if(args.length < 1) {
      printUsage
    }    
    var beginStep = 1
    var listMode = false
    while(args(0).startsWith("-")) {
      args(0) match {
        case "-s" => {
          beginStep = args(1).toInt
          args = args.drop(2)
        }
        case "-l" => {
          listMode = true
          args = args.drop(1)
        }
        case _ => printUsage
      }
    }
    val programSB = new StringBuilder()
    val ln = System.getProperty("line.separator")
    programSB.append("import nimrod._ ; ")
    programSB.append("import nimrod.tasks._ ; ")
    programSB.append("import java.io._ ; ")
    programSB.append("implicit val workflow = new Workflow(\""+args(0)+"\") ; ")
    programSB.append("val opts = new Opts(Array[String](" + args.drop(1).map("\""+_+"\"").mkString(",") + ")) ; ")
    for(line <- io.Source.fromFile(args(0)).getLines) {
      programSB.append(line + ln)
    }
    if(listMode) {
      programSB.append("workflow.list " + ln)
    } else {
      programSB.append("workflow.start(" + beginStep + ")" + ln)
    }
    Preprocessor(programSB)
    try {
      new Eval()(programSB.toString())
    } catch {
      case x : WorkflowException => System.err.println(x.getMessage())
      case x : Eval.CompilerException => {
        System.err.println("The scripts has the following errors:")
        System.err.println(x.getMessage())
      }
    }
  }
}
