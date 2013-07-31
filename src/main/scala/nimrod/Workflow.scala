package nimrod

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
