package nimrod.tasks

import nimrod._
import java.io.File

object sort {
  def apply(file : File)(implicit workflow : Workflow) = Do("sort",file.getCanonicalPath)(workflow) env ("LC_ALL","C")
  def apply(path : String)(implicit workflow : Workflow) : Do = apply(new File(path))(workflow)
}
