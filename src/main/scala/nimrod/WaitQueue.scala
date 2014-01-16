package nimrod

class WaitQueue[M] extends Iterator[M] {
  private var finished = false
  private var queue = collection.mutable.Queue[M]()

  def ! (m : M) = this.synchronized {    
    queue.enqueue(m)
    this.notify()
  }

  def stop = finished = true

  def hasNext = this.synchronized {
    !finished || !queue.isEmpty
  }

  def next : M = if(queue.isEmpty) {
     if(finished) {
       throw new NoSuchElementException()
     } else {
       this.synchronized {
         this.wait()
       }
       return next
     }
  } else {
    this.synchronized {
      if(!queue.isEmpty) {
        return queue.dequeue()
      }
    }
    return next
  }
}

