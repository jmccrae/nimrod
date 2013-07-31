package nimrod

import java.io._

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
    val proc = pb.start()
  //  pb.inheritIO()
    stdin match {
      case Some(file) => new Connector(new FileInputStream(file),proc.getOutputStream(),true).start()
      case None => {}
    }
    stdout match {
      case Some(file) => new Connector(proc.getInputStream(),new FileOutputStream(file),true).start()
      case None => new Connector(proc.getInputStream(),System.out).start()
    }
     stderr match {
      case Some(file) => new Connector(proc.getErrorStream(),new FileOutputStream(file),true).start()
      case None => new Connector(proc.getErrorStream(),System.err).start()
    }
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

  class Connector(in : InputStream, out : OutputStream, close : Boolean = false) extends Thread {
    override def run() {
        try {
          var i = 0
          while({ i = in.read(); i != -1}) {
            out.write(i)
          }
        } catch {
          case x : EOFException => {}
        }
        if(close) {
          out.flush
          out.close
        }
    }
  }
}

object Do {
  def apply(cmd : String, args : String*)(implicit workflow : Workflow) = workflow.register(new Do(cmd :: args.toList))
}
