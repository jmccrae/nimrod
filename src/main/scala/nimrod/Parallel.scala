package nimrod

import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class ThreadPool(nThreads : Int, action : Int => Block) extends Task {
  override def exec = {
    val success = collection.mutable.Map[Int,Boolean]()
    val tpe = new ThreadPoolExecutor(nThreads,nThreads,10,TimeUnit.SECONDS,new SynchronousQueue[Runnable]())
    for(i <- 1 to nThreads) {
      println("Start head " + i)
      val task = new Runnable() {
        def run = {
          if(action(i).exec != 0) {
            success.put(i,false)
          } else {
            success.put(i,true)
          }
        }
      }
      tpe.execute(task)
    }
    tpe.awaitTermination(5,TimeUnit.DAYS)
    tpe.shutdown()
    0
  }
}

object threadPool {
  def apply(nThreads : Int, name : String)(action : Int => Unit)(implicit workflow : Workflow) {
    val tp = new ThreadPool(nThreads, i => {
      block(name + " (" + i + ")")({
        action(i)
      })(workflow)
    })
    workflow.register(tp)
  }
}
