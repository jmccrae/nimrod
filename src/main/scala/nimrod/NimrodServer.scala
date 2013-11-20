package nimrod

import java.io._
import java.net._

class NimrodServer(port : Int) {

  def start = {
    val serverSocket = new ServerSocket(port)
    System.err.println("Nimrod server listening on port %d" format port)
    while(true) {
      val socket = serverSocket.accept()
      new Thread(new NimrodServerThread(socket)).start()
    }
  }

  def handle(command : String, out : PrintStream, in : ControlledInputStream) = command match {
    case "EXIT" => System.exit(-1)
    case _ => out.println("Unrecognized command")
  }
  
  private class NimrodServerThread(socket : Socket) extends Runnable {
    def run = {
      val out = new PrintStream(new ControlledOutputStream(socket.getOutputStream))
      val in = new ControlledInputStream(socket.getInputStream)
      val bb = new collection.mutable.ListBuffer[Byte]()
      var b : Int = 0
      while({b = in.read() ; b != -1}) {
        if(b == -2) {
          handle(new String(bb.toArray),out,in)
        } else {
          bb.append(b.toByte)
        }
      }
      out.flush
      socket.close
    }
  }
}

class ControlledOutputStream(base : OutputStream) extends OutputStream {
  def write(b : Int) = {
    if(b == 120) {
      base.write(b)
      base.write(b)
    } else {
      base.write(b)
    }
  }

  def eos = {
    base.write(120)
    base.write(121)
  }
}

class ControlledInputStream(base : InputStream) extends InputStream {
  def read() = {
    base.read() match {
      case 120 => base.read() match {
        case 121 => -2
        case 120 => 120
        case -1 => -1
        case _ => throw new IOException("Invalid control character")
      }
      case b => b
    }
  }
}
