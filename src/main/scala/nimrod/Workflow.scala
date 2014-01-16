package nimrod

import akka.actor._


/**
 * Represents the workflow of the script, an implicit instance is available throughout the script
 */
class Workflow(val name : String, val key : String) extends TaskMessenger {
  private var tasks : List[Task] = Nil

  def register[T <: Task](task : T) : T = {
    tasks ::= task
    task
  }

  def register(context : Context) : Task = {
    val task = new Task {
      override def exec = { context.workflow.start(1, Workflow.this.messenger.getOrElse(throw new RuntimeException("Messenger not set when calling sub-context"))) ; 0 }
      def name = "Subtask"
      protected def messenger = Workflow.this.messenger.getOrElse(throw new RuntimeException("Messenger not set when calling sub-context"))
    }
    context.workflow.currentStep = currentStep.push
    tasks ::= task
    task
  }

  def update[T <: Task](old : T, task : T) : T = {
    tasks = tasks map (t => {
      if(t eq old) {
        task
      } else {
        t
      }
    })
    task
  }

  def reset { 
    tasks = Nil 
  }

  var currentStep = Step(List((0,0)))
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

  private var messenger : Option[Messenger] = None

  def print(text : String) = messenger.map(_.print(text))
  def println(text : String) = messenger.map(_.println(text))
  object err extends CanPrint {
    def print(text : String) = messenger.map(_.print(text))
    def println(text : String) = messenger.map(_.println(text))
  }

  def list(msg : Messenger = DefaultMessenger) {
    messenger = Some(msg)
    currentStep = currentStep set (1, tasks.size)
    for(task <- tasks.reverse) {
      msg.println("  " + currentStep + ". " + task.toString)
      currentStep += 1
    }
  }

  def start(step : Int, msg : Messenger = DefaultMessenger) {
    if(tasks.isEmpty) {
      throw new WorkflowException("No tasks defined")
    }
    messenger = Some(msg)
    currentStep = currentStep set(step, tasks.size)
    for(task <- tasks.reverse.drop(step-1)) {      
      for(req <- task.requirements) {
        if(!req.validInput) {
          throw new WorkflowException("Requirement not satisified " + req)
        }
      }
      
      msg.startTask(task, currentStep)
      var errorCode = task.exec
      if(errorCode != 0) {
        msg.failTask(task, errorCode, currentStep)
        return
      }
      msg.endTask(task, currentStep)
      currentStep += 1
    }    
  }
  
  def compileFail(message : String) {
    System.err.println(message)
    System.exit(-1)
  }
}

/*class WorkflowActor(workflow : Workflow) extends Actor {
  def receive = {
    case ListTasks => {
      try {
        workflow.list(new AkkaMessenger(sender, workflow.key))
      } catch {
        case WorkflowException(msg,_) => {
          sender ! WorkflowNotStarted(workflow.key, msg)
        }
      }
      sender ! Completion(workflow.key)
    }
    case StartWorkflow(step : Int) => {
      try {
        workflow.start(step, new AkkaMessenger(sender, workflow.key))
      } catch {
        case WorkflowException(msg,_) => {
          sender ! WorkflowNotStarted(workflow.key, msg)
        }
      }
      sender ! Completion(workflow.key)
    }
  }
}*/

class WorkflowActor(workflow : Workflow) extends WaitQueue[Message] {
  def list {
    try {
      workflow.list(new AkkaMessenger(this, workflow.key))
    } catch {
      case WorkflowException(msg, _) => {
        this ! WorkflowNotStarted(workflow.key, msg)
      }
    }
    stop
  }
  def start(step : Int) {
    try {
      workflow.start(step, new AkkaMessenger(this, workflow.key))
    } catch {
      case WorkflowException(msg, _) => {
        this ! WorkflowNotStarted(workflow.key, msg)
      }
    }
    stop
  }
}



trait CanPrint {
  def print(text : String) : Unit
  def println(text : String) : Unit
}

trait TaskMessenger extends CanPrint {
  def err : CanPrint
}

trait Messenger extends TaskMessenger {
  def startTask(task : Task, step : Step) : Unit
  def endTask(task : Task, step : Step) : Unit
  def failTask(task : Task, errorCode : Int, step : Step) : Unit
}

class AkkaMessenger(actor : WaitQueue[Message], key : String) extends Messenger {
  def startTask(task : Task, step : Step) = actor ! TaskStarted(key, task.toString(), step)
  def endTask(task : Task, step : Step) = actor ! TaskCompleted(key, task.toString(), step)
  def failTask(task : Task, errorCode : Int, step : Step) = actor ! TaskFailed(key, task.toString(), errorCode, step)
  def print(text : String) = actor ! StringMessage(key, text)
  def println(text : String) = actor ! StringMessage(key, text,true)
  object err extends CanPrint {
    def print(text : String) = actor ! StringMessage(key, text,false,true)
    def println(text : String) = actor ! StringMessage(key, text,true,true)
  }
}

object DefaultMessenger extends Messenger {
  def startTask(task : Task, step : Step) = System.out.println("[\033[0;32m " + step + " \033[m] Start: " + task)
  def endTask(task : Task, step : Step) = System.out.println("[\033[0;32m " + step + " \033[m] Finished: " + task)
  def failTask(task : Task, errorCode : Int, step : Step) = System.out.println("[\033[0;31m " + step + " \033[m] Failed [" + errorCode + "]: " + task)
  def print(text : String) = System.out.print(text)
  def println(text : String) = System.out.println(text)
  object err extends CanPrint {
    def print(text : String) = System.err.print(text)
    def println(text : String) = System.err.println(text)
  }
}

/** Thrown if the workflow could not be executed (not if the execution failed) */
case class WorkflowException(msg : String = null, cause : Exception = null) extends RuntimeException(msg,cause)

case class Step(val number : List[(Int,Int)]) {
  require(!number.isEmpty)
  def +(n : Int) = number match {
    case (x,y) :: steps => Step((x+n,y) :: steps)
    case _ => throw new RuntimeException("Unreachable")
  }
  def incTotal(n : Int = 1) = number match {
    case (x,y) :: steps => Step((x,y+n) :: steps)
    case _ => throw new RuntimeException("Unreachable")
  }
  def set(n : Int, N : Int) = number match {
    case (x,y) :: steps => Step((n,N) :: steps)
    case _ => throw new RuntimeException("Unreachable")
  }
  def push = Step((0,0) :: number)
  override def toString = number.map(_._1).mkString(".") + " / " + number.map(_._2).mkString(".")
}
