package nimrod

import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket._
import io.netty.channel.socket.nio._
import io.netty.handler.codec._
import io.netty.handler.codec.serialization._

class NettyServerHandler(call : Message => Iterator[Message]) extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx : ChannelHandlerContext, msg : AnyRef) {
    msg match {
      case m : Message => for(result <- call(m)) {
        ctx.writeAndFlush(result).sync()
      }
      case x => System.err.println("Unknown message " + x)
    }
  }

  override def exceptionCaught(ctx : ChannelHandlerContext, cause : Throwable) {
    cause.printStackTrace()
    ctx.close()
  }
}

class NettyServer(port : Int, call : Message => Iterator[Message]) {
  val bossGroup = new NioEventLoopGroup()
  val workerGroup = new NioEventLoopGroup()

  val b = new ServerBootstrap()
  b.group(bossGroup, workerGroup).
  channel(classOf[NioServerSocketChannel]).
  childHandler(new ChannelInitializer[SocketChannel] {
    def initChannel(ch : SocketChannel) {
      ch.pipeline().addLast(new NettyStringEncoder())
      ch.pipeline().addLast(new NettyStringDecoder())
      ch.pipeline.addLast(new NettyServerHandler(call))
    }
  }).
  option(ChannelOption.SO_BACKLOG, new Integer(128)).
  childOption(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)

  val f = b.bind(port).sync()

  def await() { f.channel().closeFuture().sync() }

  def stop {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }
}

class NettyClientHandler(call : Message => Unit) extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx : ChannelHandlerContext, msg : AnyRef) {
    msg match {
      case m : Message => {
        println("received " + m)
        call(m)
      }
      case _ => System.err.println("Did not understand server response " + msg)
    }
  }
}

class NettyClient(hostName : String, port : Int, call : Message => Unit) {
  val workerGroup = new NioEventLoopGroup()

  val b = new Bootstrap()
  b.group(workerGroup).
    channel(classOf[NioSocketChannel]).
    option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE).
    handler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch : SocketChannel) {
        ch.pipeline().addLast(new NettyStringDecoder())
        ch.pipeline().addLast(new NettyStringEncoder())
        ch.pipeline().addLast(new NettyClientHandler(call))
      }
    })

  val f = b.connect(hostName, port).sync()

  val channel = f.channel()

  def send(msg : Object) = channel.writeAndFlush(msg).sync()

  def stop = workerGroup.shutdownGracefully()
}

class NettyStringDecoder extends MessageToMessageDecoder[ByteBuf] {
  private def readString(msg : ByteBuf) = {
    val size = msg.readInt()
    val buf = new Array[Byte](size)
    msg.readBytes(buf)
    new String(buf)
  }

  private def readStringList(msg : ByteBuf) = {
    val size = msg.readInt()
    (for(i <- 1 to size) yield {
      readString(msg)
    }).toList
  }

  private def readStep(msg : ByteBuf) = {
    val size = msg.readInt()
    Step((for(i <- 1 to size) yield {
      (msg.readInt(),msg.readInt())
    }).toList)
  }

  override def decode(ctx : ChannelHandlerContext, msg : ByteBuf, out : java.util.List[Object]) {
    msg.readInt() match {
      case 0 => out.add(ListTasks)
      case 1 => out.add(StartWorkflow(msg.readInt()))
      case 2 => out.add(Completion(readString(msg)))
      case 3 => out.add(WorkflowNotStarted(readString(msg), readString(msg)))
      case 4 => out.add(NimrodTaskSubmission(readString(msg), readString(msg), readStringList(msg), msg.readBoolean(), msg.readInt()))
      case 5 => out.add(TaskStarted(readString(msg), readString(msg), readStep(msg)))
      case 6 => out.add(TaskCompleted(readString(msg), readString(msg), readStep(msg)))
      case 7 => out.add(TaskFailed(readString(msg), readString(msg), msg.readInt(), readStep(msg)))
      case 8 => out.add(StringMessage(readString(msg), readString(msg), msg.readBoolean(), msg.readBoolean()))
      case _ => System.err.println("Invalid message to decode")
    }
  }
}

class NettyStringEncoder extends ChannelOutboundHandlerAdapter {
  private def writeString(buf : ByteBuf, str : String) {
    val bytes = str.toString().getBytes()
    buf.capacity(buf.capacity() + 4 + bytes.size)
    buf.writeInt(bytes.size)
    buf.writeBytes(bytes)
  }

  private def writeInt(buf : ByteBuf, int : Int) {
    buf.capacity(buf.capacity() + 4)
    buf.writeInt(int)
  }

  private def writeBoolean(buf : ByteBuf, bool : Boolean) {
    buf.capacity(buf.capacity() + 1)
    buf.writeBoolean(bool)
  }

  private def writeStep(buf : ByteBuf, step : Step) {
    writeInt(buf, step.number.size)
    for((i,j) <- step.number) {
      writeInt(buf, i)
      writeInt(buf, j)
    }
  }

  override def write(ctx : ChannelHandlerContext, msg : Object, promise : ChannelPromise) {
    val buf = ctx.alloc().buffer(4)
    msg match {
      case ListTasks => {
        buf.writeInt(0)
      }
      case StartWorkflow(step) => {
        buf.writeInt(1)
        writeInt(buf, step)
      }
      case Completion(key) => {
        buf.writeInt(2)
        writeString(buf, key)
      }
      case WorkflowNotStarted(key, msg) => {
        buf.writeInt(3)
        writeString(buf, key)
        writeString(buf, msg)
      }
      case NimrodTaskSubmission(name, program, args, listMode, beginStep) => {
        buf.writeInt(4)
        writeString(buf, name)
        writeString(buf, program)
        writeInt(buf, args.length)
        for(arg <- args) {
          writeString(buf, arg)
        }
        writeBoolean(buf, listMode)
        writeInt(buf, beginStep)
      }
      case TaskStarted(key, name, step) => {
        buf.writeInt(5)
        writeString(buf, key)
        writeString(buf, name)
        writeStep(buf, step)
      }
      case TaskCompleted(key, name, step) => {
        buf.writeInt(6)
        writeString(buf, key)
        writeString(buf, name)
        writeStep(buf, step)
      }
      case TaskFailed(key, name, errorCode, step) => {
        buf.writeInt(7)
        writeString(buf, key)
        writeString(buf, name)
        writeInt(buf, errorCode)
        writeStep(buf, step)
      }
      case StringMessage(key, text, nl, err) => {
        buf.writeInt(8)
        writeString(buf, key)
        writeString(buf, text)
        writeBoolean(buf, nl)
        writeBoolean(buf, nl)
      }
    }
    System.err.println("encoded " + msg)
    ctx.write(buf, promise)
  }
}

