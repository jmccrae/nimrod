package nimrod

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
}
