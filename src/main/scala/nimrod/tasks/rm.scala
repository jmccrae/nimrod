package nimrod.tasks
import nimrod._
import java.io.File

class rm(file : File, protected val messenger : TaskMessenger) extends Task {
  private var e = false
  private var recursive = false
  override def exec = if(recursive && file.isDirectory()) {
    if(delTree(file)) {
      0
    } else {
      -1
    }
  } else if(file.delete() || e) {
    0
  } else {
    -1
  }
  requires(file)

  private def delTree(file : File) : Boolean = {
    for(f <- file.listFiles) {
      if(f.isDirectory()) {
        (delTree(f)) || (return false)
      } else {
        (f.delete()) || (return false)
      }
    }
    return file.delete()
  }

  def ifexists = { 
    unrequire(file)
    e = true 
    this
  }

  def r = {
    recursive = true
    this
  }

  override def toString = "rm " + file
}

object rm {
  def apply(path : String)(implicit workflow : Workflow) = workflow.register(new rm(new File(path), workflow))
  def apply(file : File)(implicit workflow : Workflow) = workflow.register(new rm(file, workflow))
}
