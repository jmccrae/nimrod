package nimrod
import java.io.File

class Do(args : List[String]) extends Task {
  private var stdin : Option[File] = None
  private var stdout : Option[File] = None
  private var stderr : Option[File] = None

  private val cmdFile = new File(args(0))
  if(cmdFile.exists) {
    requires(cmdFile)
  } else {
    val paths = System.getenv("PATH").split(System.getProperty("path.separator"))
    val validPath = paths find (path => {
      new File(path + System.getProperty("file.separator") + args(0)).exists
    })
    validPath match {
      case Some(f) => requires(new File(f))
      case None => throw new WorkflowException("Command not found: " + args(0))
    }
  }

  override def exec = {
    val pb = new ProcessBuilder(args:_*)
    pb.inheritIO()
    stdin match {
      case Some(file) => pb.redirectInput(file)
      case None =>
    }
    stdout match {
      case Some(file) => pb.redirectOutput(file)
      case None =>
    }
     stderr match {
      case Some(file) => pb.redirectError(file)
      case None =>
    }
     val proc = pb.start()
    proc.waitFor()
    proc.exitValue
  }

  def <(file : File) = { 
    stdin = Some(file)
    requires(file)
    this 
  }
  def <(path : String) =  { 
    val file = new File(path)
    stdin = Some(file)
    requires(file)
    this 
  }
  def >(file : File) = { 
    stdout = Some(file)
    generates(new FileArtifact(file))
    this 
  }
  def >(path : String) =  { 
    val f = new File(path)
    stdout = Some(f)
    generates(new FileArtifact(f))
    this 
  }
  def err(file : File) = { 
    stderr = Some(file)
    this 
  }
  def err(path : String) =  { 
    stderr = Some(new File(path)) 
    this 
  }
  
  override def toString = args.mkString(" ")
}

object Do {
  def apply(cmd : String, args : String*)(implicit workflow : Workflow) = workflow.register(new Do(cmd :: args.toList))
}
