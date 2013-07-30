package nimrod

import java.io.File

trait Task {
  var requirements : List[Artifact] = Nil
  var results : List[Artifact] = Nil
  def exec : Int = 0
  protected def requires(artifact : Artifact) {
    requirements ::= artifact
  }
  protected def requires(artifact : File) {
    requirements ::= new FileArtifact(artifact)
  }
  protected def generates(artifact : Artifact) {
    results ::= artifact
  }
  protected def generates(artifact : File) {
    results ::= new FileArtifact(artifact)
  }
}

trait Monitorable {
  private var N = 0
  protected def setPips(n : Int) { N = n }
  def pips = N
  def pip = System.err.print(".")
  def done = System.err.println()
}

trait Run {
  def dateForFile(file : File) : Long = 0l
}

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

class Workflow(val name : String) {
  private var tasks : List[Task] = Nil
  private var block : Option[Block] = None

  def register[T <: Task](task : T) : T = {
    block match {
      case Some(b) => b.add(task)
      case None => tasks ::= task
    }
    task
  }

  def update[T <: Task](old : T, task : T) : T = {
    block match {
      case Some(b) => b.update(old,task)
      case None => tasks = tasks map (t => {
        if(t eq old) {
          task
        } else {
          t
        }
      })
    }
    task
  }

  def reset { 
    tasks = Nil 
  }

  var currentStep = 0
  def totalSteps = tasks.size
  
  private def pad(i : Int) = {
    if(i < 10) {
      "  " +i
    } else if (i < 100) {
      " " + i
    } else {
      i.toString
    }
  }

  def start {
    if(tasks.isEmpty) {
      throw new WorkflowException("No tasks defined")
    }
    currentStep = 1
    for(task <- tasks.reverse) {      
      for(req <- task.requirements) {
        if(!req.validInput) {
          throw new WorkflowException("Requirement not satisified " + req)
        }
      }
      
      println("\033[0;32m[ " + (currentStep) + " / " + (tasks.size) + " ] Start: " + task + "\033[m")
      if(task.exec != 0) {
        println("\033[0;31m[ " + (currentStep) + " / " + (tasks.size) + " ] Failed: " + tasks + "\033[m")
        return
      }
      println("\033[0;32m[ " + (currentStep) + " / " + (tasks.size) + " ] Finished: " + task + "\033[m")
      currentStep += 1
    }    
  }
  
  def compileFail(message : String) {
    System.err.println("Workflow failed to compile:")
    System.err.println(message)
    System.exit(-1)
  }

  def startBlock(b : Block) = {
    tasks ::= (b)
    block = Some(b)
  }
  def endBlock(b : Block) = {
    block = None
  }
}

class WorkflowException(msg : String = null, cause : Exception = null) extends RuntimeException(msg,cause)
