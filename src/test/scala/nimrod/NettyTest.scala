package nimrod

import org.scalatest._

class NettyTest extends FlatSpec with Matchers {
  "netty communication" should "send basic message" in {
    val timer = new java.util.Timer()
    val server = new NettyServer(5555, x => { println("msg:" + x) ; Some(x).iterator })
    val client : NettyClient = new NettyClient("localhost", 5555, x => x match {
      case s : StringMessage => timer.schedule(new java.util.TimerTask {
        def run { server.stop }
      }, 1000)
      case _ => println(x)
    })
    client.send(ListTasks)
    System.err.println("Sending second message")
    client.send(StartWorkflow(10))
    client.send(WorkflowNotStarted("x","y"))
    client.send(NimrodTaskSubmission("x","y","z" :: Nil, false, 0))
    client.send(TaskStarted("key","name",Step((0,0) :: Nil)))
    client.send(TaskCompleted("key","name",Step((0,0) :: Nil)))
    client.send(TaskFailed("key","name",-1,Step((0,0) :: Nil)))
    client.send(StringMessage("key","value",true,true))
    timer.schedule(new java.util.TimerTask {
      def run { client.stop }
    }, 1000)
  }
}
