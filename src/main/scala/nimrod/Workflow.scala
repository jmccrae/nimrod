package nimrod

/**
 * Represents the workflow of the script, an implicit instance is available throughout the script
 */
class Workflow(val name : String, val key : String) extends TaskMessenger {
  private var tasks : List[Task] = Nil

  /** Add a task to the workflow */
  def register[T <: Task](task : T) : T = {
    tasks ::= task
    task
  }

  /** Add a set of tasks as a subtask to the workflow */
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

  /** Replace a task in the workflow */
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

  /** Remove all tasks from the workflow */
  def reset { 
    tasks = Nil 
  }

  /** Where the workflow is at the moment */
  var currentStep = Step(List((0,0)))
  /** How many tasks this workflow will execute (subcontexts count as 1 */
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

  /** List all tasks in the workflow (send the result back to the messenger */
  def list(msg : Messenger = DefaultMessenger) {
    messenger = Some(msg)
    currentStep = currentStep set (1, tasks.size)
    for(task <- tasks.reverse) {
      msg.println("  " + currentStep + ". " + task.toString)
      currentStep += 1
    }
  }

  /** Start the workflow
   * @param step The step to start at
   * @param msg Where to send messages as generates
   */
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
  
  /**
   * Fail the current building of the workflow (use cautiously)
   */
  def compileFail(message : String) {
    System.err.println(message)
    System.exit(-1)
  }
}

/**
 * The actor for this workflow. Iterate this to listen to messages
 */
class WorkflowActor(workflow : Workflow) extends WaitQueue[Message] {
  def list {
    try {
      workflow.list(new ActorMessenger(this, workflow.key))
    } catch {
      case WorkflowException(msg, _) => {
        this ! WorkflowNotStarted(workflow.key, msg)
      }
    }
    stop
  }
  def start(step : Int) {
    try {
      workflow.start(step, new ActorMessenger(this, workflow.key))
    } catch {
      case WorkflowException(msg, _) => {
        this ! WorkflowNotStarted(workflow.key, msg)
      }
    }
    stop
  }
}

/** An iterator that blocks on next until a value is available */
class WaitQueue[M] extends Iterator[M] {
  private var finished = false
  private var queue = collection.mutable.Queue[M]()

  def ! (m : M) = this.synchronized {    
    queue.enqueue(m)
    this.notify()
  }

  def stop = finished = true

  def hasNext = this.synchronized {
    !finished || !queue.isEmpty
  }

  def next : M = if(queue.isEmpty) {
     if(finished) {
       throw new NoSuchElementException()
     } else {
       this.synchronized {
         this.wait()
       }
       return next
     }
  } else {
    this.synchronized {
      if(!queue.isEmpty) {
        return queue.dequeue()
      }
    }
    return next
  }
}

/** Thrown if the workflow could not be executed (not if the execution failed) */
case class WorkflowException(msg : String = null, cause : Exception = null) extends RuntimeException(msg,cause)

/** A step in this workflow */
case class Step(val number : List[(Int,Int)]) {
  require(!number.isEmpty)
  /** Move forward n steps */
  def +(n : Int) = number match {
    case (x,y) :: steps => Step((x+n,y) :: steps)
    case _ => throw new RuntimeException("Unreachable")
  }
  /** Increase the number of steps to do */
  def incTotal(n : Int = 1) = number match {
    case (x,y) :: steps => Step((x,y+n) :: steps)
    case _ => throw new RuntimeException("Unreachable")
  }
  /** Move into substep (n, N) */
  def set(n : Int, N : Int) = number match {
    case (x,y) :: steps => Step((n,N) :: steps)
    case _ => throw new RuntimeException("Unreachable")
  }
  /** Move into substep (0,0) */
  def push = Step((0,0) :: number)
  override def toString = number.map(_._1).mkString(".") + " / " + number.map(_._2).mkString(".")
}
