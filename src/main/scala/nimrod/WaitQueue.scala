package nimrod

/**
 * Something you can print to
 */
trait CanPrint {
  def print(text : String) : Unit
  def println(text : String) : Unit
}

/**
 * Something with an err stream as well as the standard out
 */
trait TaskMessenger extends CanPrint {
  def err : CanPrint
}

/**
 * A messenger that can receive messages
 */
trait Messenger extends TaskMessenger {
  def startTask(task : Task, step : Step) : Unit
  def endTask(task : Task, step : Step) : Unit
  def failTask(task : Task, errorCode : Int, step : Step) : Unit
}

/**
 * A messenger that posts to a wait queue
 */
class ActorMessenger(actor : WaitQueue[Message], key : String) extends Messenger {
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

/**
 * A messenger that prints to STDOUT/STDERR
 */
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


