package nimrod

sealed trait Message
trait KeyedMessage extends Message {
  def key : String
}

object ListTasks extends Message
case class StartWorkflow(step : Int) extends Message
case class Completion(val key : String) extends KeyedMessage
case class WorkflowNotStarted(val key : String, msg : String) extends KeyedMessage
case class NimrodTaskSubmission(name : String, program : String, args : List[String], listMode : Boolean, beginStep : Int) extends Message
case class TaskStarted(val key : String, name : String, step : Step) extends KeyedMessage
case class TaskCompleted(val key : String, name : String, step : Step) extends KeyedMessage
case class TaskFailed(val key : String, name : String, errorCode : Int, step : Step) extends KeyedMessage
case class StringMessage(val key : String, text : String, nl : Boolean = false, err : Boolean = false) extends KeyedMessage
