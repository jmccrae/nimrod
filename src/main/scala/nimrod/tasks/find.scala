package nimrod.tasks

import nimrod.{Workflow, TaskMessenger}
import java.io.File

class find(start : File, filter : File => Boolean, val messenger : TaskMessenger) {
  def apply : Seq[File] = findFrom(start)

  def findFrom(file : File) : Seq[File] = {
  if(file.isDirectory) {
    if(filter(file)) {
      Seq(file)
    } else {
      (for(file <- file.listFiles) yield {
        findFrom(file)
      }).toSeq.flatten
    }
  } else {
    if(filter(file)) {
      Seq(file)
    } else {
      Seq()
    }
  }
  }
}

object find {
  def apply(file : File)(filter : File => Boolean)(implicit workflow : Workflow) = new find(file,filter,workflow)
  def apply(path : String)(filter : File => Boolean)(implicit workflow : Workflow) = new find(new File(path),filter,workflow)
}
