package nimrod

trait Artifact {
  def uptodate(run : Run) : Boolean
  def validInput : Boolean
  def validOutput : Boolean
  def toParameters : List[String]
}

case class FileArtifact(file : File) extends Artifact {
  def uptodate(run : Run) = file.lastModified() <= run.dateForFile(file) 
  def validInput = file.exists() && file.canRead()
  def validOutput = !file.exists() || file.canWrite()
  def toParameters = List(file.getCanonicalPath())
}

class GenericArtifact[E] extends Artifact {
  private var e : Option[E] = None
  def uptodate(run : Run) = e != None
  def validInput = e != None
  def validOutput = true
  def toParameters = List(e.toString)
}


