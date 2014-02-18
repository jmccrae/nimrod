package nimrod

import com.twitter.util.Eval
import java.io.File

/**
 * A single task in a workflow
 */
trait Task extends TaskMessenger {
  var requirements : List[Artifact] = Nil
  var results : List[Artifact] = Nil
  /** Add objects to this list to trigger clean up when 
   * all tasks in the current workflow have
   * been completed */
  var taskResources : List[TaskResource] = Nil
  /**
   * Execute a single task
   * @return 0 for success, non-zero value for fail
   */
  final def exec : Int = run

  /** Clean up all resources at the end of a workflow */
  def cleanUp = {
    for(tr <- taskResources) {
      tr.taskComplete
    }
  }

  /** Implement to define what exec does */
  protected def run : Int

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
    def run = { action ; 0 }
    override def toString = name
    protected val messenger = workflow
  })
  /** Add all task from a context as subclasses */
  def apply(context : Context)(implicit workflow : Workflow) = workflow.register(context)
}

object result {
  /** Add a function as a task which returns a result */
  def apply[T](name : String)(action : => T)(implicit workflow : Workflow) : Result[T] = {
    val result = new Result[T]()
    workflow.register(new Task {
      def run = { result.set(action) ; 0 }
      override def toString = name
      protected val messenger = workflow
    })
    result
  }
}

/**
 * Result is a task with a result, which is used later in the program
 */
class Result[T] {
  private var value : Option[T] = None
  /** Get the result
   * @throws NoSuchElementException If the task is not yet complete */
  def apply() = value.get
  /** Complete the task */
  def set(t : T) {
    value = Some(t)
  }
  /** Complete the task. Same as set */
  def :=(t : T) {
    value = Some(t)
  }
}

trait TaskResource {
  def taskComplete : Unit
}
  
