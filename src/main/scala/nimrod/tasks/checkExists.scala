package nimrod.tasks

import nimrod._
import java.io.File

class checkExists(file : File) extends Task {
  override def exec = 0
 
  requires(file)
  
  override def toString = "Check Exists " + file
}

object checkExists {
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new checkExists(file))
  def apply(path : String)(implicit workflow : Workflow) = workflow.register(new checkExists(new File(path)))
}
