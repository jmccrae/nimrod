package nimrod

import java.io._

class Do(args : List[String], wf : TaskMessenger) extends Task {
  private var stdin : Option[File] = None
  private var stdout : Option[File] = None
  private var stdoutAppend : Boolean = false
  private var stderr : Option[File] = None
  private var _dir : Option[File] = None
  private val envs = collection.mutable.Map[String,String]()

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

  protected def messenger = wf

  def run = {
    val pb = new ProcessBuilder(args:_*)
    for((key,value) <- envs) {
      pb.environment().put(key,value)
    }
    if(_dir != None) {
      pb.directory(_dir.get)
    }
    var connectors : List[Connector] = Nil
    val proc = pb.start()
  //  pb.inheritIO()
    stdin match {
      case Some(file) => {
        val c = new Connector(new FileInputStream(file),proc.getOutputStream(),true)
        connectors ::= c
        c.start()
      }
      case None => {}
    }
    stdout match {
      case Some(file) => {
        val c = new Connector(proc.getInputStream(),new FileOutputStream(file,stdoutAppend),true)
        connectors ::= c
        c.start()
      }
      case None => {
        val c = new Connector(proc.getInputStream(),System.out)
        connectors ::= c
        c.start()
      }
    }
    stderr match {
      case Some(file) => {
        val c = new Connector(proc.getErrorStream(),new FileOutputStream(file),true)
        connectors ::= c
        c.start()
      }
      case None => {
        val c = new Connector(proc.getErrorStream(),System.err)
        connectors ::= c 
        c.start()
      }
    }
    proc.waitFor()
    for(connector <- connectors) {
      if(connector.isAlive()) {
        connector.join()
      }
    }
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
  def >>(file : File) = { 
    stdout = Some(file)
    stdoutAppend = true
    generates(new FileArtifact(file))
    this 
  }
  def >>(path : String) =  { 
    val f = new File(path)
    stdout = Some(f)
    stdoutAppend = true
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
  def env(key : String, value : String) = {
    envs.put(key,value)
    this
  }
  def dir(file : File) = {
    _dir = Some(file)
    this
  }
  def dir(path : String) : Do = dir(new File(path))
  
  override def toString = args.mkString(" ")

  class Connector(in : InputStream, out : OutputStream, close : Boolean = false) extends Thread {
    override def run() {
        try {
          var i = 0
          val buf = new Array[Byte](1024)
          while({ i = in.read(buf); i != -1}) {
            out.write(buf,0,i)
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
  def apply(cmd : String, args : String*)(implicit workflow : Workflow) = workflow.register(new Do(cmd :: args.toList, workflow))
}
