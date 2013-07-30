package nimrod.tasks

import nimrod._

class mkdir(path : String, subDirs : Boolean) extends Task {
  override def exec = {
    if(subDirs) {
      new java.io.File(path).mkdirs
    } else {
      new java.io.File(path).mkdir
    }
    0
  }

  def p(implicit workflow : Workflow) = workflow.update(this,new mkdir(path,true))

  override def toString = "mkdir " + path
}

object mkdir {
  def apply(path : String)(implicit workflow : Workflow) = workflow.register(new mkdir(path,false))
}
