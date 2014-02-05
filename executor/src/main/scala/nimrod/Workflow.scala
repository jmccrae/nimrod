package nimrod

/**
 * Represents the workflow of the script, an implicit instance is available throughout the script
 */
class Workflow(val name : String, val key : String) extends Messenger {
  private var tasks : List[Task] = Nil

  /** Add a task to the workflow */
  def register[T <: Task](task : T) : T = {
    tasks ::= task
    currentStep.incTotal()
    task
  }

  /** Add a set of tasks as a subtask to the workflow */
  def register(context : Context) : Task = {
    val task = new Task {
      def run = { context.workflow.start(1, Workflow.this.messenger.getOrElse(throw new RuntimeException("Messenger not set when calling sub-context"))) ; 0 }
      def name = "Subtask"
      protected def messenger = Workflow.this.messenger.getOrElse(throw new RuntimeException("Messenger not set when calling sub-context"))
      override def toString = context.toString
      override def cleanUp = context.cleanUp
    }
    val total = context.workflow.currentStep.total
    context.workflow.currentStep = currentStep.push
    context.workflow.currentStep.incTotal(total)
    currentStep.incTotal()
    tasks ::= task
    task
  }

  /** Clean-up this workflow (do not call!) */
  def cleanUp {
    for(task <- tasks) {
      task.cleanUp
    }
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
  var currentStep = new Step()
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
  def startTask(task : Task, step : Step) = messenger.map(_.startTask(task, step))
  def endTask(task : Task, step : Step) = messenger.map(_.endTask(task, step))
  def failTask(task : Task, errorCode : Int, step : Step) = messenger.map(_.failTask(task, errorCode, step))
  def complete = messenger.map(_.complete)
  def monitorReset(n : Int) = messenger.map(_.monitorReset(n))
  def pip = messenger.map(_.pip)


  /** List all tasks in the workflow (send the result back to the messenger */
  def list(msg : Messenger = DefaultMessenger) {
    messenger = Some(msg)
    currentStep := 1
    try {
      for(task <- tasks.reverse) {
        msg.println("  " + currentStep + ". " + task.toString)
        currentStep += 1
      }
    } finally {
      cleanUp
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
    currentStep := step
    try {
      for(task <- tasks.reverse.drop(step-1)) {      
        for(req <- task.requirements) {
          if(!req.validInput) {
            throw new WorkflowException("Requirement not satisified " + req)
          }
        }
        
        msg.startTask(task, currentStep.copy)
        val errorCode = try {
          task.exec 
        } catch {
          case t : Throwable => {
            t.printStackTrace()
            256
          }
        }
        if(errorCode != 0) {
          msg.failTask(task, errorCode, currentStep.copy)
          return
        }
        msg.endTask(task, currentStep.copy)
        currentStep += 1
      }    
    } finally {
      cleanUp
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
    } finally {
      stop
    }
  }
  def start(step : Int) {
    try {
      workflow.start(step, new ActorMessenger(this, workflow.key))
    } catch {
      case WorkflowException(msg, _) => {
        this ! WorkflowNotStarted(workflow.key, msg)
      }
    } finally {
      stop
    }
  }
}

/** An iterator that blocks on next until a value is available.
 * It is assumed that only a single thread iterates this object, but many threads may insert
 * 
 */
class WaitQueue[M] extends Iterator[M] {
  private var finished = false
  private var queue = collection.mutable.Queue[M]()

  def ! (m : M) = {
    if(finished) {
      throw new IllegalStateException("Cannot add to a wait queue, which is already finished")
    } else {
      this.synchronized {    
        queue.enqueue(m)
        this.notify()
      }
    }
  }

  def stop = this.synchronized { 
    finished = true
    this.notify()
  }

  def hasNext : Boolean = {
    if(queue.isEmpty) {
      if(finished) {
        return false
      } else {
        // Wait for the next element or stop signal
        this.synchronized {
          this.wait()
        }
      }
      // Either finished has been set or a new element is in the queue
      return !finished || !queue.isEmpty
    } else {
      return true
    }
  }

  def next : M = {
    if(queue.isEmpty) {
      if(finished) {
        // Queue is empty and we are finished (no more elements)
        throw new NoSuchElementException()
      } else {
        // Empty but not finished wait
        this.synchronized {
          this.wait()
        }
        // Did we finish?
        if(!finished) {
          return this.synchronized {
            queue.dequeue()
          }
        } else {
          throw new NoSuchElementException()
        }
      }
    } else {
      return this.synchronized {
        queue.dequeue()
      }
    }
  }
}

/** Thrown if the workflow could not be executed (not if the execution failed) */
case class WorkflowException(msg : String = null, cause : Exception = null) extends RuntimeException(msg,cause)

/** A step in this workflow */
class Step(next : Option[Step] = None) {
  private var _step : Int = 0
  private var _total : Int = 0
  def step = _step
  def total = _total
  require(!number.isEmpty)
  def number : List[(Int,Int)] = next match {
    case Some(n) => (_step, _total) :: n.number
    case None => (_step, _total) :: Nil
  }
  /** Move forward n steps */
  def +=(n : Int) { _step += n }
  /** Increase the number of steps to do */
  def incTotal(n : Int = 1) { _total += n }
  /** Move into substep (0,0) */
  def push = new Step(Some(this))
  /** Set the current step */
  def :=(n : Int) { _step = n }
  /** Set the current step and total */
  def set(n : Int, N : Int) = {
    _step = n
    _total = N
    this
  }
  /** Make a copy of this */
  def copy : Step = new Step(next.map(_.copy)).set(_step, total)
  override def toString = number.reverse.map(_._1).mkString(".") + " / " + number.reverse.map(_._2).mkString(".")
}

object Step {
  def apply(elems : List[(Int,Int)]) : Step = {
    elems match {
      case (n,m) :: Nil => new Step(None).set(n, m)
      case (n,m) :: xs => new Step(Some(Step(xs))).set(n, m)
      case Nil => throw new RuntimeException("Empty step list")
    }
  }
}
