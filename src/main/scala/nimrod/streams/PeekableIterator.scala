package nimrod.streams

class PeekableIterator[A](base : Iterator[A]) extends Iterator[A] {
  private var last : Option[A] = None

  override def next = last match {
    case Some(a) => {
      last = None
      a
    }
    case None => base.next
  }

  override def hasNext = last != None || base.hasNext

  def peek : Option[A] = {
    last match {
      case Some(_) => 
      case None => {
        if(base.hasNext) {
          last = Some(base.next)
        }
      }
    }
    last
  }
}

