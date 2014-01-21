package nimrod

import com.twitter.util.Eval
import java.io.File

/**
 * A single task in a workflow
 */
trait Task extends TaskMessenger {
  var requirements : List[Artifact] = Nil
  var results : List[Artifact] = Nil
  /**
   * Execute a single task
   * @return 0 for success, non-zero value for failer
   */
  def exec : Int = 0
  /** Indicate this task requires the given artifact */
  protected def requires(artifact : Artifact) {
    requirements ::= artifact
  }
  /** Indicate this task requires the given artifact */
  protected def requires(artifact : File) {
    requirements ::= new FileArtifact(artifact)
  }
  /** Indicate this task does not require the given artifact */
  protected def unrequire(artifact : Artifact) {
    requirements = requirements.filterNot(_ == artifact)
  }
  /** Indicate this task does not require the given artifact */
  protected def unrequire(artifact : File) {
    requirements = requirements.filterNot({
      case FileArtifact(f) => artifact == f
      case _ => false
    })
  }  
  /** Indicate this task generates the given artifact */
  protected def generates(artifact : Artifact) {
    results ::= artifact
  }
  /** Indicate this task generates the given artifact */
  protected def generates(artifact : File) {
    results ::= new FileArtifact(artifact)
  }
  /** Where to send messages */
  protected def messenger : TaskMessenger
  def print(text : String) = messenger.print(text)
  def println(text : String) = messenger.println(text)
  object err extends CanPrint {
    def print(text : String) = messenger.err.print(text)
    def println(text : String) = messenger.err.println(text)
  }
}

object task {
  /** Create a task with a given name and action in the workflow */
  def apply(name : String)(action : => Unit)(implicit workflow : Workflow) = workflow.register(new Task {
    override def exec = { action ; 0 }
    override def toString = name
    protected val messenger = workflow
  })
  /** Add all task from a context as subclasses */
  def apply(context : Context)(implicit workflow : Workflow) = workflow.register(context)
}
