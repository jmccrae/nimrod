package nimrod

import com.twitter.util.Eval

object Main {
  def main(args : Array[String]) {
    if(args.length < 1) {
      System.err.println("Usage: ./nimrod script.scala [args]")
      System.exit(-1)
    }    
    val programSB = new StringBuilder()
    val ln = System.getProperty("line.separator")
    programSB.append("import nimrod._ ; ")
    programSB.append("import nimrod.tasks._ ; ")
    programSB.append("import java.io._ ; ")
    programSB.append("implicit val workflow = new Workflow(\""+args(0)+"\") ; ")
    programSB.append("val args = Array[String](" + args.drop(1).map("\""+_+"\"").mkString(",") + ") ; ")
    for(line <- io.Source.fromFile(args(0)).getLines) {
      programSB.append(line + ln)
    }
    programSB.append("workflow.start" + ln)
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
