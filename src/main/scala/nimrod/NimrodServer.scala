package nimrod

import com.twitter.util.Eval
import java.util.concurrent._

/**
 * The Nimrod Engine controls the execution of a workflows
 */
object NimrodEngine {
  /** Generate a random key */
  def genkey = scala.util.Random.nextInt.toHexString

  /** 
   * Submit a workflow
   * @param name The name of the workflow
   * @param program The program text
   * @param args The command line arguments
   * @param listMode List don't execute
   * @param beginStep Where to start
   */
  def submit(name : String, program : String, args : List[String], listMode : Boolean, beginStep : Int) : Iterator[Message] = {
    val executor = Executors.newSingleThreadExecutor()
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
      val workflowActor = new WorkflowActor(workflow)
      if(listMode) {
        executor.execute(new Runnable {
          def run { workflowActor.list }
        })
      } else {
        executor.execute(new Runnable {
          def run { workflowActor.start(beginStep) }
        })
      }
      executor.shutdown()
        workflowActor
      } catch {
        case x : Eval.CompilerException => {
          Seq(WorkflowNotStarted(name, x.getMessage())).iterator
        }
      }
  }

  /**
   * Interpret a message as something to show to a user on the command line
   */
  def cli(msg : Message) = msg match {
    case WorkflowNotStarted(key, msg) => {
      System.err.println(msg)
    }
    case TaskStarted(_, name, step) => System.out.println("[\033[0;32m " + step + " \033[m] Start: " + task)
    case TaskCompleted(_, name, step) => System.out.println("[\033[0;32m " + step + " \033[m] Finished: " + task)
    case TaskFailed(_, name, errorCode, step) => System.out.println("[\033[0;31m " + step + " \033[m] Failed [" + errorCode + "]: " + task)
    case StringMessage(_, text, true, true) => System.err.println(text)
    case StringMessage(_, text, false, true) => System.err.print(text)
    case StringMessage(_, text, true, false) => System.out.println(text)
    case StringMessage(_, text, false, false) => System.out.print(text)
  }

  /**
   * Exeucte a workflow locally
   */
  def local(name : String, args : List[String], listMode : Boolean, beginStep : Int) = {
    val ln = System.getProperty("line.separator")
    val programSB = new StringBuilder()
    for(line <- scala.io.Source.fromFile(name).getLines()) {
      programSB.append(line + ln)
    }
    for(msg <- submit(name, programSB.toString(), args, listMode, beginStep)) {
      cli(msg)
    }
  }
}

/**
 * Create a remote Nimrod server
 * @param port The port to listen at
 */
class NimrodServer(port : Int) {
  private val server = new NettyServer(port, msg => {
    msg match {
      case NimrodTaskSubmission(name, program, args, listMode, beginStep) => NimrodEngine.submit(name, program, args, listMode,
        beginStep)
      case _ => throw new RuntimeException("bad message " + msg)
    }
  })
  /** Block until the server finishes (which never happens normally) */
  def await() = server.await()
}

/**
 * Create a Nimrod remote client
 * @param server The name of the remote server
 * @param port The port to listen at
 */
class NimrodClient(server : String, port : Int) {
  private val client = new NettyClient(server, port, NimrodEngine.cli)

  /** Submit a task to the remote server */
  def submit(name :String, args : List[String], listMode : Boolean, beginStep : Int) = {
    val ln = System.getProperty("line.separator")
    val programSB = new StringBuilder()
    for(line <- scala.io.Source.fromFile(name).getLines()) {
      programSB.append(line + ln)
    }
    client send NimrodTaskSubmission(name, programSB.toString(), args, listMode, beginStep)
  }
}
