package nimrod.tasks
import nimrod._
import java.io.File

class rm(file : File) extends Task {
  override def exec = if(file.delete()) {
    0
  } else {
    -1
  }
  requires(file)

  override def toString = "rm " + file
}

object rm {
  def apply(path : String)(implicit workflow : Workflow) = workflow.register(new rm(new File(path)))
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new rm(file))
}
