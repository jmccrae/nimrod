package nimrod

import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class ThreadPool(nThreads : Int, action : Int => Block) extends Task {
  override def exec = {
    val success = collection.mutable.Map[Int,Boolean]()
    val tpe = new ThreadPoolExecutor(nThreads,nThreads,10,TimeUnit.SECONDS,new SynchronousQueue[Runnable]())
    for(i <- 1 to nThreads) {
      val block = action(i)
      val task = new Runnable() {
        def run = {
          if(block.exec != 0) {
            success.put(i,false)
          } else {
            success.put(i,true)
          }
        }
      }
      tpe.execute(task)
    }
    tpe.shutdown()
    tpe.awaitTermination(5,TimeUnit.DAYS)
    0
  }

  override def toString = "Concurrent task on " + nThreads + " threads"
}

object threadPool {
  def apply(nThreads : Int, name : String)(action : Int => Unit)(implicit workflow : Workflow) {
    val tp = new ThreadPool(nThreads, i => {
      val b = new Block(name + "(" + i + ")",workflow)
      workflow.startBlock(b,false)
      action(i)
      workflow.endBlock(b)
      b
    })
    workflow.register(tp)
  }
}
