package nimrod

import java.io.{File, PrintStream}

trait Artifact {
  def validInput : Boolean
  def validOutput : Boolean
}

case class FileArtifact(file : File) extends Artifact {
  def validInput = file.exists() && file.canRead()
  def validOutput = !file.exists() || file.canWrite()
  def asStream : PrintStream = new PrintStream(file)
  def asSource : scala.io.Source = scala.io.Source.fromFile(file)
}

class GenericArtifact[E](e : Option[E]) extends Artifact {
  def validInput = e != None
  def validOutput = true
}

