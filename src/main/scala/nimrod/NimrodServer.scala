package nimrod

import akka.actor._
import akka.remote.RemoteScope
import com.typesafe.config.ConfigFactory
import com.twitter.util.Eval

object BounceToMe

class NimrodServerActor extends Actor {
  private var returns = collection.mutable.Map[String, ActorRef]()
  def genkey = scala.util.Random.nextInt.toHexString

  def receive = {
    case NimrodTaskSubmission(name, program, args, listMode, beginStep) => {
      val programSB = new StringBuilder()
      val key = genkey

      val ln = System.getProperty("line.separator")
      programSB.append("import nimrod._ ; ")
      programSB.append("import nimrod.tasks._ ; ")
      programSB.append("import java.io._ ; ")
      programSB.append("object ThisContext extends nimrod.Context {")
      programSB.append("def name = \""+name+"\";")
      programSB.append("def args = Seq(" + args.map("\""+_+"\"").mkString(",") + ");")
      programSB.append("override def key = \""+key+"\";")
      programSB.append(program + ln)
      programSB.append("};")
      programSB.append("ThisContext.workflow")
      try {
        val workflow = new Eval()(programSB.toString()).asInstanceOf[Workflow]
        val workflowActor = context.actorOf(Props(classOf[WorkflowActor], workflow), "workflow-" + workflow.key)
        if(listMode) {
          workflowActor ! ListTasks
        } else {
          workflowActor ! StartWorkflow(beginStep)
        }
        returns += key -> sender
      } catch {
        case x : WorkflowException => {
          sender ! WorkflowNotStarted(name, x.getMessage()) 
        }
        case x : Eval.CompilerException => {
          sender ! WorkflowNotStarted(name, x.getMessage())
        }
      }
    }
    case WorkflowNotStarted(key, msg) => {
      returns.get(key) match {
        case Some(actor) => actor ! WorkflowNotStarted(key, msg)
        case None => System.err.println("Could not send message to actor")
      }
    }
    case Completion(key) => {
      returns.get(key) match {
        case Some(actor) => {
          actor ! Completion(key)
          returns -= key
        }
        case None => {
          System.err.println("Could not send completion to actor " + key)
        }
      }
    }
    case msg : KeyedMessage => returns.get(msg.key).map(_ ! msg)
    case BounceToMe => // noop
  }
}

class NimrodLocalActor(server : String, port : Int) extends Actor {
  val remote = context.actorSelection("akka.tcp://nimrod@"+server+":"+port+"/user/server")
  var bounce : Option[ActorRef] = None
  def receive = {
    case s : NimrodTaskSubmission => remote ! s
    case Completion(key) => {
      context.system.shutdown()
    }
    case BounceToMe => {
      bounce = Some(sender)
    }
    case msg : KeyedMessage => {
      bounce.map(_ ! msg)
    }
  }
}

class NimrodCLIActor(server : ActorRef) extends Actor {
  def receive = {
    case s : NimrodTaskSubmission => {
      server ! BounceToMe
      server ! s
    }
    case Completion(key) => {
      context.system.shutdown()
    }
    case WorkflowNotStarted(key, msg) => {
      System.err.println(msg)
      context.system.shutdown()
    }
    case TaskStarted(_, name, step) => System.out.println("[\033[0;32m " + step + " \033[m] Start: " + task)
    case TaskCompleted(_, name, step) => System.out.println("[\033[0;32m " + step + " \033[m] Finished: " + task)
    case TaskFailed(_, name, errorCode, step) => System.out.println("[\033[0;31m " + step + " \033[m] Failed [" + errorCode + "]: " + task)
    case StringMessage(_, text, true, true) => System.err.println(text)
    case StringMessage(_, text, false, true) => System.err.print(text)
    case StringMessage(_, text, true, false) => System.out.println(text)
    case StringMessage(_, text, false, false) => System.out.print(text)
  }
}

class NimrodEngine {
  private var system : ActorSystem = null
  private var actor : ActorRef = null

  def startServer(port : Int) {
    val conf = ConfigFactory.parseString("""akka {
  log-dead-letters-during-shutdown = false
  actor {
    provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
    transport = "akka.remote.netty.NettyRemoteTransport"
    netty.tcp {
      hostname = "127.0.0.1"
      port = """ + port + """
    }
  }
}""")

    system = ActorSystem("nimrod", ConfigFactory.load(conf))

    actor = system.actorOf(Props[NimrodServerActor], "server")
  }

  def startLocal() {
    system = ActorSystem("nimrod")

    actor = system.actorOf(Props[NimrodServerActor], "local")
  }

  def startRemote(server : String, port : Int) {
    system = ActorSystem("local", ConfigFactory.load(ConfigFactory.parseString("""
akka {
  actor.provider = "akka.remote.RemoteActorRefProvider"
  remote.transport = "akka.remote.netty.NettyRemoteTransport"
  remote.netty.tcp.hostname = "127.0.0.1"
  remote.netty.tcp.port = 0 
  log-dead-letters-during-shutdown = false
}""")))
    
    actor = system.actorOf(Props(classOf[NimrodLocalActor],server,port), "remoter")
  }

  def submit(name : String, args : List[String], listMode : Boolean, beginStep : Int) = {
    require(system != null)
    val cliActor = system.actorOf(Props(classOf[NimrodCLIActor], actor), "cli")
    val programSB = new StringBuffer()
    val ln = System.getProperty("line.separator")
    for(line <- io.Source.fromFile(name).getLines()) {
      programSB.append(line + ln)
    }
    cliActor ! NimrodTaskSubmission(name, programSB.toString(), args, listMode, beginStep)
  }

  def stopServer {
    if(system != null) {
      system.shutdown()
    }
  }
}
    

