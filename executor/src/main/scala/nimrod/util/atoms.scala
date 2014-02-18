package nimrod.util

import java.lang.Double.{doubleToLongBits, longBitsToDouble}
import java.lang.Float.{floatToIntBits, intBitsToFloat}
import java.util.concurrent.atomic._

package object atoms {
  implicit def atomFloat2Float(f : AtomFloat) = f.get
  implicit def atomDouble2Double(d : AtomDouble) = d.get
  implicit def atomInt2Int(i : AtomInt) = i.get
  implicit def atom2Object[T](o : Atom[T]) = o.get
}

package atoms {
  /**
   * Not implemented in Java, use AtomFloat for Scala niceties. Thanks to aioobe at StackOverflow for implementation
   */
  class AtomicFloat(initialValue : Float = 0f) extends Number {
    private val bits = new AtomicInteger(floatToIntBits(initialValue))

    def compareAndSet(expect : Float, update : Float) = {
      bits.compareAndSet(floatToIntBits(expect),
                         floatToIntBits(update))
    }

    def set(newValue : Float) = {
      bits.set(floatToIntBits(newValue));
    }

    def get() = {
      intBitsToFloat(bits.get());
    }

    def floatValue() = get()

    def getAndSet(newValue : Float) = {
      intBitsToFloat(bits.getAndSet(floatToIntBits(newValue)));
    }

    def weakCompareAndSet(expect : Float, update : Float) = {
      bits.weakCompareAndSet(floatToIntBits(expect),
                             floatToIntBits(update))
    }

    def doubleValue() = floatValue().toDouble
    def intValue() = floatValue().toInt
    def longValue() = floatValue().toLong

    def getAndAdd(i : Float) : Float = {
      while(true) {
        val x = get()
        val y = x + i
        if(compareAndSet(x, y)) {
          return y
        }
      }
      throw new RuntimeException("Unreachable")
    }
  }

  /**
   * Not implemented in Java, use AtomDouble for Scala niceties. Thanks to aioobe at StackOverflow for implementation
   */
  class AtomicDouble(initialValue : Double = 0d) extends Number {
    private val bits = new AtomicLong(doubleToLongBits(initialValue))

    def compareAndSet(expect : Double, update : Double) = {
      bits.compareAndSet(doubleToLongBits(expect),
                         doubleToLongBits(update))
    }

    def set(newValue : Double) = {
      bits.set(doubleToLongBits(newValue));
    }

    def get() = {
      longBitsToDouble(bits.get());
    }

    def doubleValue() = get()

    def getAndSet(newValue : Double) = {
      longBitsToDouble(bits.getAndSet(doubleToLongBits(newValue)));
    }

    def weakCompareAndSet(expect : Double, update : Double) = {
      bits.weakCompareAndSet(doubleToLongBits(expect),
                             doubleToLongBits(update))
    }

    def floatValue() = doubleValue().toFloat
    def intValue() = doubleValue().toInt
    def longValue() = doubleValue().toLong
  
    def getAndAdd(i : Double) : Double = {
      while(true) {
        val x = get()
        val y = x + i
        if(compareAndSet(x, y)) {
          return y
        }
      }
      throw new RuntimeException("Unreachable")
    }
}

  /**
   * A version of AtomicInteger as in Java but with Scala niceties
   */
  class AtomInt(initialValue : Int = 0) {
    private val atom = new AtomicInteger(initialValue)

    /** Increment this value */
    def +=(i : Int) { atom.getAndAdd(i) }
    /** Decrement this value */
    def -=(i : Int) { atom.getAndAdd(-i) }
    /** Set this value */
    def :=(i : Int) { atom.set(i) }
    /** Get this value */
    def get = atom.get()
    /** Apply this function to the value. Note this implementation
     * is non-blocking so function may be called many times.
     * It is often better to use proper synchronization! */
    def :==(foo : Int => Int) : Int = { 
      while(true) {
        val x = atom.get()
        val y = foo(x)
        if(atom.compareAndSet(x, y)) {
          return y
        }
      }
      throw new RuntimeException("Unreachable")
    }
  }

  object AtomInt {
    def apply(initialValue : Int = 0) = new AtomInt(initialValue)
  }

  /**
   * A version of AtomicFloat as in Java but with Scala niceties
   */
  class AtomFloat(initialValue : Float = 0) {
    private val atom = new AtomicFloat(initialValue)

    /** Increment this value */
    def +=(i : Float) { atom.getAndAdd(i) }
    /** Decrement this value */
    def -=(i : Float) { atom.getAndAdd(-i) }
    /** Set this value */
    def :=(i : Float) { atom.set(i) }
    /** Get this value */
    def get = atom.get()
    /** Apply this function to the value. Note this implementation
     * is non-blocking so function may be called many times.
     * It is often better to use proper synchronization! */
    def :==(foo : Float => Float) : Float = { 
      while(true) {
        val x = atom.get()
        val y = foo(x)
        if(atom.compareAndSet(x, y)) {
          return y
        }
      }
      throw new RuntimeException("Unreachable")
    }
  }

  object AtomFloat {
    def apply(initialValue : Float = 0) = new AtomFloat(initialValue)
  }

  /**
   * A version of AtomicFloat as in Java but with Scala niceties
   */
  class AtomDouble(initialValue : Double = 0) {
    private val atom = new AtomicDouble(initialValue)

    /** Increment this value */
    def +=(i : Double) { atom.getAndAdd(i) }
    /** Decrement this value */
    def -=(i : Double) { atom.getAndAdd(-i) }
    /** Set this value */
    def :=(i : Double) { atom.set(i) }
    /** Get this value */
    def get = atom.get()
    /** Apply this function to the value. Note this implementation
     * is non-blocking so function may be called many times.
     * It is often better to use proper synchronization! */
    def :==(foo : Double => Double) : Double = { 
      while(true) {
        val x = atom.get()
        val y = foo(x)
        if(atom.compareAndSet(x, y)) {
          return y
        }
      }
      throw new RuntimeException("Unreachable")
    }
  }

  object AtomDouble {
    def apply(initialValue : Double = 0.0) = new AtomDouble(initialValue)
  }

  /**
   * A version of AtomicReference as in Java but with Scala niceties
   */
  class Atom[T](initialValue : T = null) {
    private val atom = new AtomicReference(initialValue)

    /** Get this value */
    def get = atom.get()
    /** Set this value */
    def :=(t : T) { atom.set(t) }
    /** Apply this function to the value. Note this implementation
     * is non-blocking so function may be called many times.
     * It is often better to use proper synchronization */
    def :==[S <: T](foo : T => S) : S = {
      while(true) {
        val x = atom.get()
        val y = foo(x)
        if(atom.compareAndSet(x, y)) {
          return y
        }
      }
      throw new RuntimeException("Unreachable")
    }
  }

  object Atom {
    def apply[T](t : T = null) = new Atom(t)
  }
}
