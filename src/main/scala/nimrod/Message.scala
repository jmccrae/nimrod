package nimrod

sealed trait Message
trait KeyedMessage extends Message {
  def key : String
}

object ListTasks extends Message
case class StartWorkflow(step : Int) extends Message
case class Completion(val key : String) extends KeyedMessage
case class WorkflowNotStarted(val key : String, msg : String) extends KeyedMessage
