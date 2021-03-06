package nimrod.tasks
import nimrod._
import java.io.File

class gunzip(file : File) extends Do(List("zcat","-c",file.getCanonicalPath)) {
  requires(file)
  val targetFile = new File(file.getCanonicalPath().replaceAll(".gz$",""))
  generates(new FileArtifact(targetFile))

  this > targetFile

  override def toString = "gunzip " + file
}

object gunzip {
  def apply(path : String)(implicit wf : Workflow) = if(path endsWith (".gz")) {
    wf.register(new gunzip(new File(path)))
  } else {
    throw new IllegalArgumentException("Unknown suffix: " + path)
  }
  def apply(file : File)(implicit wf : Workflow) = if(file.getCanonicalPath() endsWith (".gz")) {
    wf.register(new gunzip(file))
  } else {
    throw new IllegalArgumentException("Unknown suffix: " + file.getPath())
  }
}
