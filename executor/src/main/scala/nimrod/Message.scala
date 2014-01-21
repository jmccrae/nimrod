package nimrod

/**
 * A message to (possibly remotely) be sent
 */
sealed trait Message
/**
 * A message with a workflow identifier (key)
 */
trait KeyedMessage extends Message {
  def key : String
}

/** List all task */
object ListTasks extends Message
/** Start a workflow */
case class StartWorkflow(step : Int) extends Message
/** Sent on workflow completion */
case class Completion(val key : String) extends KeyedMessage
/** Sent when a workflow could not be started */
case class WorkflowNotStarted(val key : String, msg : String) extends KeyedMessage
/** Submit a program to be compiled into a workflow */
case class NimrodTaskSubmission(name : String, program : String, args : List[String], listMode : Boolean, beginStep : Int) extends Message
/** Sent when a task starts */
case class TaskStarted(val key : String, name : String, step : Step) extends KeyedMessage
/** Sent when a task completes */
case class TaskCompleted(val key : String, name : String, step : Step) extends KeyedMessage
/** Sent when a task fails */
case class TaskFailed(val key : String, name : String, errorCode : Int, step : Step) extends KeyedMessage
/** Sent when a task says something */
case class StringMessage(val key : String, text : String, nl : Boolean = false, err : Boolean = false) extends KeyedMessage
/** Poll a server for a message */
case class Poll(val key : String) extends KeyedMessage
/** Start a server/client communication */
case class Start(val key : String) extends KeyedMessage
/** Poison pill to kill a server */
object Die extends Message
