package nimrod

import org.scalatest._

class NettyTest extends FlatSpec with Matchers {
/*  "netty communication" should "send basic message" in {
    val server = new NettyServer(5555, x => { println("msg:" + x) ; Some(x).iterator })
    val client : NettyClient = new NettyClient("localhost", 5555, x => { })
    client.send(ListTasks)
    System.err.println("Sending second message")
    client.send(StartWorkflow(10))
    client.send(WorkflowNotStarted("x","y"))
    client.send(NimrodTaskSubmission("x","y","z" :: Nil, false, 0))
    client.send(TaskStarted("key","name",Step((0,0) :: Nil)))
    client.send(TaskCompleted("key","name",Step((0,0) :: Nil)))
    client.send(TaskFailed("key","name",-1,Step((0,0) :: Nil)))
    client.send(StringMessage("key","value",true,true))
    client.send(Die)
  }*/
}