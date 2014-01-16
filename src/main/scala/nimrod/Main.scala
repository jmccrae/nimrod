package nimrod

object Main {
  private def printUsage = {
    System.err.println("Usage: ./nimrod script.scala [args]")
    System.exit(-1)
  }

  def main(_args : Array[String]) {
    if(_args.length == 2 && _args(0) == "-d") {
      val port = try {
        _args(1).toInt
      } catch {
        case nfx : NumberFormatException => {
          printUsage
          0
        }
      }
//      new NimrodEngine().startServer(port)
      new NimrodServer(port).await()
    } else {
      var args = _args.toList
      if(args.length < 1) {
        printUsage
      }    
      var beginStep = 1
      var listMode = false
      var server = ""
      var port = -1
      while(args(0).startsWith("-")) {
        args(0) match {
          case "-s" => {
            beginStep = args(1).toInt
            args = args.drop(2)
          }
          case "-l" => {
            listMode = true
            args = args.drop(1)
          }
          case "-r" => {
            args(1) split ":" match {
              case Array(s,p) => {
                server = s
                port = p.toInt
                args = args.drop(2)
              }
            }
          }
          case _ => printUsage
        }
      }
      if(port > 0) { // Submit to remote instance
//        nimrod.startRemote(server, port)
        new NimrodClient(server, port).submit(args(0), args.drop(1), listMode, beginStep)
      } else {
        NimrodEngine.local(args(0), args.drop(1), listMode, beginStep)
//        nimrod.startLocal()
      }
//      nimrod.submit(args(0), args.drop(1), listMode, beginStep) 
    }
  }
}
