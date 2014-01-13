package nimrod

import com.twitter.util.Eval
import java.io.File

/**
 * A single task in a workflow
 */
trait Task extends TaskMessenger {
  var requirements : List[Artifact] = Nil
  var results : List[Artifact] = Nil
  def exec : Int = 0
  protected def requires(artifact : Artifact) {
    requirements ::= artifact
  }
  protected def requires(artifact : File) {
    requirements ::= new FileArtifact(artifact)
  }
  protected def unrequire(artifact : Artifact) {
    requirements = requirements.filterNot(_ == artifact)
  }
  protected def unrequire(artifact : File) {
    requirements = requirements.filterNot({
      case FileArtifact(f) => artifact == f
      case _ => false
    })
  }  
  protected def generates(artifact : Artifact) {
    results ::= artifact
  }
  protected def generates(artifact : File) {
    results ::= new FileArtifact(artifact)
  }
  protected def messenger : TaskMessenger
  def print(text : String) = messenger.print(text)
  def println(text : String) = messenger.println(text)
  object err extends CanPrint {
    def print(text : String) = messenger.err.print(text)
    def println(text : String) = messenger.err.println(text)
  }
}

trait Run {
  def dateForFile(file : File) : Long = 0l
}

trait Artifact {
  def uptodate(run : Run) : Boolean
  def validInput : Boolean
  def validOutput : Boolean
  def toParameters : List[String]
}

case class FileArtifact(file : File) extends Artifact {
  def uptodate(run : Run) = file.lastModified() <= run.dateForFile(file) 
  def validInput = file.exists() && file.canRead()
  def validOutput = !file.exists() || file.canWrite()
  def toParameters = List(file.getCanonicalPath())
}

class GenericArtifact[E] extends Artifact {
  private var e : Option[E] = None
  def uptodate(run : Run) = e != None
  def validInput = e != None
  def validOutput = true
  def toParameters = List(e.toString)
}

object task {
  def apply(name : String)(action : => Unit)(implicit workflow : Workflow) = workflow.register(new Task {
    override def exec = { action ; 0 }
    override def toString = name
    protected val messenger = workflow
  })
  def apply(context : Context)(implicit workflow : Workflow) = workflow.register(context)
}

/*object subTask {
  def apply(file : File, opts : String*)(implicit workflow : Workflow) {
    val programSB = new StringBuilder()
    val ln = System.getProperty("line.separator")
    programSB.append("import nimrod._ ; ")
    programSB.append("import nimrod.tasks._ ; ")
    programSB.append("import java.io._ ; ")
    programSB.append("implicit val workflow = new Workflow(\""+file.getPath()+"\") ; ")
    programSB.append("val opts = new Opts(Array[String](" + opts.map("\""+_+"\"").mkString(",") + ")).doNotRequireFileExists ; ")
    for(line <- io.Source.fromFile(file).getLines) {
      programSB.append(line + ln)
    }
    programSB.append("workflow")
    val wf = new Eval().apply[Workflow](programSB.toString())
    workflow.add(wf)
  }
  def apply(path : String, opts : String*)(implicit workflow : Workflow) { apply(new File(path),opts:_*)(workflow) }
}*/
