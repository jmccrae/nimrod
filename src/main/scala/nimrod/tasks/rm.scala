package nimrod.tasks
import nimrod._
import java.io.File

class rm(file : File) extends Task {
  private var e = false
  override def exec = if(file.delete() || e) {
    0
  } else {
    -1
  }
  requires(file)

  def ifexists { 
    unrequire(file)
    e = true 
  }
  override def toString = "rm " + file
}

object rm {
  def apply(path : String)(implicit workflow : Workflow) = workflow.register(new rm(new File(path)))
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new rm(file))
}
