package nimrod.services

import java.lang.annotation.Annotation
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.HashMap
import java.util.Collection

/**
 *
 * @author John McCrae
 */
class InjectableClass[C](val clazz : Class[C]) {
  val constructor = if (clazz.getConstructors().length == 1) {
    clazz.getConstructors()(0)
  } else {
    throw new ServiceLoadException(clazz, clazz.getName() + " does not have a marked or single constructor");
  }
  for (_type <- constructor.getGenericParameterTypes()) {
    _type match {
      case pt : ParameterizedType => {
        if (!(pt.getRawType().isInstanceOf[Class[_]])) {
          throw new ServiceLoadException(clazz, "Bad type on constructor argument " + pt);
        }
        if ((classOf[Collection[_]].isAssignableFrom(pt.getRawType().asInstanceOf[Class[_]]) ||
              classOf[Seq[_]].isAssignableFrom(pt.getRawType().asInstanceOf[Class[_]])) &&
            !(pt.getActualTypeArguments()(0).isInstanceOf[Class[_]])) {
            throw new ServiceLoadException(clazz, "Bad type on constructor argument " + pt);
        }
      } 
      case cl : Class[_] => {
      }
      case _ => {
        throw new ServiceLoadException(clazz, "Bad type on constructor argument " + _type);
      }
    }
  }

   

    /**
     * Get the real type of the argument
     *
     * @param t The type
     * @return Either the class t represents or the arguments
     */
    def getRealType(t : Type) = t match {
      case c : Class[_] => c
      case pt : ParameterizedType if InjectableClass.isMultiple(t) => pt.getActualTypeArguments()(0)
      case pt : ParameterizedType => pt.getRawType()
    }

    def dependencies = constructor.getGenericParameterTypes();

    def newInstance(args : Array[Object]) : C = {
      try {
        constructor.newInstance(args : _*).asInstanceOf[C]
      } catch {
        case x : IllegalAccessException => throw new ServiceLoadException(clazz, x.getMessage, x);
        case x : InstantiationException => throw new ServiceLoadException(clazz, x.getMessage, x);
        case x : InvocationTargetException => throw new ServiceLoadException(clazz, x.getMessage, x);
        case x : IllegalArgumentException => throw new ServiceLoadException(clazz, x.getMessage, x);
      }
    }

    def getClassName() = clazz.getName()

    override def equals(obj : Any) : Boolean = {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      obj match {
        case other : InjectableClass[C] => {
          if (this.clazz != other.clazz && (this.clazz == null || !this.clazz.equals(other.clazz))) {
              return false;
          }
        }
      }
      return true
    }

    override def hashCode() : Int = {
      var hash = 5;
      hash = 79 * hash + (if(this.clazz != null) {this.clazz.hashCode() } else { 0 });
      return hash;
    }
}

object InjectableClass {
 /**
     * Is the type returned from {@code dependencies} multiple
     *
     * @param t The type
     * @return true if it is a ServiceCollection or Iterable
     */
    def isMultiple(t : Type) = t match {
      case pt : ParameterizedType => (pt.getRawType().asInstanceOf[Class[_]].isAssignableFrom(classOf[Collection[_]]) ||
        pt.getRawType().asInstanceOf[Class[_]].isAssignableFrom(classOf[Seq[_]])) &&
        pt.getActualTypeArguments()(0).isInstanceOf[Class[_]]
      case _ => false
    }
}
