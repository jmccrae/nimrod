package nimrod.tasks

import nimrod._
import java.io.File

class output(fileName : String, protected val messenger : TaskMessenger) extends Task {
  def run = if(new File(fileName).exists) {
    0
  } else {
    -1
  }
  
  override def toString = "Output " + fileName
}

object output {
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new output(file.getPath(), workflow))
  def apply(path : String)(implicit workflow : Workflow) = workflow.register(new output(path, workflow))
} 
