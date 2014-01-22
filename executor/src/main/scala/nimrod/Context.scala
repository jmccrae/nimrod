package nimrod

/**
 * The context used by workflows. The context defines global variables
 * usable in workflow. It can be used as follows
 *
 * val workflow = Context("my context", "arg 1", "arg 2") {
 *  // Task 1
 *  // Task 2
 * }
 * workflow.start(1)
 */
trait Context extends TaskMessenger {
  protected def name : String
  protected def args : Seq[String]
  protected def key = scala.util.Random.nextInt.toHexString
  implicit val workflow = new Workflow(name, key)
  def print(text : String) { workflow.print(text) }
  def println(text : String) { workflow.println(text) }
  object err extends CanPrint {
    def print(text : String) { workflow.err.print(text) }
    def println(text : String) { workflow.err.println(text) }
  }
  val opts = new Opts(args)
  val monitor = new MessengerProgressMonitor(workflow)
  override def toString = name
}

object Context {
  def apply(_name : String, _args : String*)(tasks : => Unit) = {
    val ctxt = new Context {
      def name = _name
      def args = _args
      tasks
    }
    ctxt.workflow
  }
}
