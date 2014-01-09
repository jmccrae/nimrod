package nimrod

import akka.actor._


/**
 * Represents the workflow of the script, an implicit instance is available throughout the script
 */
class Workflow(val name : String, val key : String) {
  private var tasks : List[Task] = Nil
  private var block : Option[Block] = None

  def add(workflow : Workflow) = {
   block match {
     case Some(b) => {
       for(task <- workflow.tasks) {
         b.add(task)
       }
     }
     case None => {
       tasks :::= workflow.tasks
     }
   }
  }

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

  def list {
    currentStep = 1
    for(task <- tasks.reverse) {
      println("  " + currentStep + ". " + task.toString)
      currentStep += 1
    }
  }

  def start(step : Int) {
    if(tasks.isEmpty) {
      throw new WorkflowException("No tasks defined")
    }
    currentStep = step
    for(task <- tasks.reverse.drop(step-1)) {      
      for(req <- task.requirements) {
        if(!req.validInput) {
          throw new WorkflowException("Requirement not satisified " + req)
        }
      }
      
      println("[\033[0;32m " + (currentStep) + " / " + (tasks.size) + " \033[m] Start: " + task)
      var errorCode = task.exec
      if(errorCode != 0) {
        println("[\033[0;31m " + (currentStep) + " / " + (tasks.size) + " \033[m] Failed [" + errorCode + "]: " + task)
        return
      }
      println("[\033[0;32m " + (currentStep) + " / " + (tasks.size) + " \033[m] Finished: " + task)
      currentStep += 1
    }    
  }
  
  def compileFail(message : String) {
    System.err.println(message)
    System.exit(-1)
  }

  def startBlock(b : Block, asTask : Boolean = true) = {
    if(asTask) {
      tasks ::= (b)
    }
    block = Some(b)
  }
  def endBlock(b : Block) = {
    block = None
  }
}

class WorkflowActor(workflow : Workflow) extends Actor {
  def receive = {
    case ListTasks => {
      try {
        workflow.list
      } catch {
        case WorkflowException(msg,_) => {
          sender ! WorkflowNotStarted(workflow.key, msg)
        }
      }
      sender ! Completion(workflow.key)
    }
    case StartWorkflow(step : Int) => {
      try {
        workflow.start(step)
      } catch {
        case WorkflowException(msg,_) => {
          sender ! WorkflowNotStarted(workflow.key, msg)
        }
      }
      sender ! Completion(workflow.key)
    }
  }
}

/** Thrown if the workflow could not be executed (not if the execution failed) */
case class WorkflowException(msg : String = null, cause : Exception = null) extends RuntimeException(msg,cause)
