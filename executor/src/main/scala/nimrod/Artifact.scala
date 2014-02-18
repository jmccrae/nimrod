package nimrod

import java.io.{File, PrintStream}

/**
 * An artifact is a resource for computation
 */
trait Artifact {
  /** Is this artifact usable as an input to the process */
  def validInput : Boolean
  /** Is this artifact usable as an output for the process */
  def validOutput : Boolean
}

/**
 * A file to be used as an artifact
 */
case class FileArtifact(file : File) extends Artifact {
  def validInput = file.exists() && file.canRead()
  def validOutput = !file.exists() || file.canWrite()
  /** 
   * Open this file to write to line by line
   * @throws ArtifactException If the file cannot be written to
   */
  def asStream : PrintStream = {
    if(!validOutput) {
      throw new ArtifactException("Invalid output artifact")
    }
    new PrintStream(file)
  }
  /**
   * Open this file as a source to read from line by line
   * @throws ArtifactException If the file cannot be read
   */
  def asSource : scala.io.Source = {
    if(!validInput) {
      throw new ArtifactException("Invalid input artifact")
    }
    scala.io.Source.fromFile(file)
  }
  /**
   * Open this file as a streamable
   * @throws ArtifactException If the file cannot be read
   */
  def lines(monitor : ProgressMonitor = NullProgressMonitor) : Streamable[Int, String] = Streamable.enumeratedIter(asSource.getLines,
    monitor=monitor)

  /**
   * Mark this artifact as temporary (delete on exit)
   */
  def temporary : FileArtifact = {
    file.deleteOnExit()
    return this
  }

  /**
   * Get the path for this artifact as a string
   */
  def pathString : String = file.getAbsolutePath()
}

object FileArtifact {
  def temporary : FileArtifact = {
    val file = File.createTempFile("nimrod", ".tmp")
    file.deleteOnExit()
    return FileArtifact(file)
  }
}

/**
 * A generic artifact wrapping an option
 */
class GenericArtifact[E](e : Option[E]) extends Artifact {
  def validInput = e != None
  def validOutput = true
}

/**
 * Indicates that an artifact cannot be used as desired
 */
class ArtifactException(msg : String = "", cause : Throwable = null) extends RuntimeException(msg, cause)
