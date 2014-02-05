package nimrod.streams

/**
 * An iterator where you can "peek" the next value, that is look at the
 * next value without advancing the iterator
 * @param base The actual iterator
 */
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

  /**
   * Peek the next value
   * @return None if there is no value or the value
   */
  def peek : Option[A] = {
    last match {
      case None => {
        if(base.hasNext) {
          last = Some(base.next)
        }
      }
      case _ =>
    }
    last
  }
}

