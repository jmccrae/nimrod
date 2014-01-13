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

object task {
  def apply(name : String)(action : => Unit)(implicit workflow : Workflow) = workflow.register(new Task {
    override def exec = { action ; 0 }
    override def toString = name
    protected val messenger = workflow
  })
  def apply(context : Context)(implicit workflow : Workflow) = workflow.register(context)
}
