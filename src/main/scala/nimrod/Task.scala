package nimrod

import java.io.File

trait Task {
  var requirements : List[Artifact] = Nil
  var results : List[Artifact] = Nil
  def exec : Int = 0
  protected def requires(artifact : Artifact) {
    requirements ::= artifact
  }
  protected def requires(artifact : File) {
    requirements ::= new FileArtifact(artifact)
  }
  protected def generates(artifact : Artifact) {
    results ::= artifact
  }
  protected def generates(artifact : File) {
    results ::= new FileArtifact(artifact)
  }
}

trait Monitorable {
  private var N = 0
  protected def setPips(n : Int) { N = n }
  def pips = N
  def pip = System.err.print(".")
  def done = System.err.println()
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
  def apply(action : => Unit)(implicit workflow : Workflow) = workflow.register(new Task {
    override def exec = { action ; 0 }
    override def toString = "Minor task "
  })
}
