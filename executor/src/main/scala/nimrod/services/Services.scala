package nimrod.services

import java.lang.reflect.Method
import java.lang.reflect.InvocationHandler
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Proxy
import java.io.IOException
import java.net.URL
import java.util.Collection

object Services {
  def get[S](clazz : Class[S]) : S = resolveImmediate(clazz)

  def getAll[S](clazz : Class[S]) : Seq[S] = resolveExtant(clazz)

  def getFactory[S](clazz : Class[S]) : S = Proxy.newProxyInstance(clazz.getClassLoader(),
    Array(clazz), new InvocationHandler() {
      lazy val services = getAll(clazz)
                               
                               
      override def invoke(o : AnyRef, method : Method, os : Array[AnyRef]) : AnyRef = {
        if (method.getDeclaringClass().equals(classOf[Object])) {
          return method.invoke(clazz, os);
        } else {
          if (method.getReturnType().equals(classOf[Collection[_]])) {
            val rval = new java.util.ArrayList[AnyRef]()
            for(s <- services) {
              val res = try {
                method.invoke(s, os);
              } catch {
                case t : Throwable => null
              }
              if(res != null) {
                rval.addAll(res.asInstanceOf[Collection[AnyRef]])
              }
            }
            return rval
          } else if(method.getReturnType().equals(classOf[Seq[_]])) {
            return services.flatMap(service => {
              try {
                method.invoke(service, os).asInstanceOf[Seq[_]]
              } catch {
                case t : Throwable => Nil
              }
            })
          } else {
            for(s <- services) {
              try {
                return method.invoke(s, os)
              } catch {
                case t : Throwable => // noop
              }
            }
            return null
          }
        }
      }
  }).asInstanceOf[S]

  val PATH_PREFIX = System.getProperty("nimrod.services.path", "META-INF/components/");
  val JSL_PATH_PREFIX = System.getProperty("nimrod.services.jslpath", "META-INF/services/");
  private val verbose = java.lang.Boolean.parseBoolean(System.getProperty("nimrod.services.verbose", "false"));

  private def resolveImmediate[S](serviceClass : Class[S]) : S = {
    var lastException : ServiceLoadException = null
    try {
      val resources = Thread.currentThread().getContextClassLoader().getResources(PATH_PREFIX + serviceClass.getName());
      while (resources.hasMoreElements()) {
        try {
          return resolveFirstURL(serviceClass, resources.nextElement(), false);
        } catch {
          case x : ServiceLoadException => lastException = x;
        }
      }
    } catch {
      case x : IOException => lastException = new ServiceLoadException(serviceClass, x.getMessage(), x);
    }
    try {
      val resources = Thread.currentThread().getContextClassLoader().getResources(JSL_PATH_PREFIX + serviceClass.getName());
      while (resources.hasMoreElements()) {
        try {
          return resolveFirstURL(serviceClass, resources.nextElement(), true);
        } catch {
          case x : ServiceLoadException => lastException = x
        }
      }
    } catch {
      case x : IOException => lastException = new ServiceLoadException(serviceClass, x.getMessage(), x)
    }
    if (lastException != null) {
      throw lastException;
    } else {
      if (verbose) {
        System.err.println("No candidate service for " + serviceClass.getName());
      }
      throw new ServiceLoadException(serviceClass);
    }
  }

  private def resolveFirstURL[S](serviceClass : Class[S], url : URL, independent : Boolean) : S = {
    try {
      val reader = scala.io.Source.fromInputStream(url.openStream()).getLines()
      var lastException : ServiceLoadException = null
      for(s <- reader) {
        val ss = s.split(";");
        try {
          val c = Thread.currentThread().getContextClassLoader().loadClass(ss(0)).asInstanceOf[Class[S]]
          try {
            if (verbose) {
              System.err.println("Binding " + ss(0) + " as " + serviceClass.getName());
            }
            return resolveSingle(serviceClass, c, independent);
          } catch {
            case x : ServiceLoadException => lastException = x;
          }
        } catch {
          case x : ClassNotFoundException => {
            if (verbose) {
              System.err.println("Failed to load class " + ss(0) + ": " + x.getMessage())
            }
            throw new ServiceLoadException(serviceClass, x.getMessage(), x)
          }
        }
      }
      if (lastException != null) {
        throw lastException;
      } else {
        if (verbose) {
          System.err.println("Empty service descriptor @ " + url);
        }
        throw new ServiceLoadException(serviceClass, "Empty service declaration @ " + url);
      }
    } catch {
      case ex : IOException => {
        if (verbose) {
          System.err.println("Error reading service descriptor " + url.toString() + ": " + ex.getMessage());
        }
        throw new ServiceLoadException(serviceClass, ex.getMessage(), ex);
      }
    }
  }

  private def  resolveSingle[S, T <: S](serviceClass : Class[S], implClass : Class[T], independent : Boolean) : S = {
    val injectableClass = new InjectableClass[T](implClass);
    if (independent && injectableClass.dependencies.length != 0) {
      if (verbose) {
        System.err.println(implClass.getName() + " does not have a single public no-args constructor");
      }
      throw new ServiceLoadException(implClass, "Class does not have a single public no-args constructor");
    }
    val arguments = new Array[AnyRef](injectableClass.dependencies.length);
    var i = 0;
    for (_type <- injectableClass.dependencies) {
      if (InjectableClass.isMultiple(_type)) {
        val clazz = _type.asInstanceOf[ParameterizedType].getActualTypeArguments()(0).asInstanceOf[Class[_]];
        try {
          arguments(i) = resolveExtant(clazz, false)
        } catch {
          case x : ServiceLoadException => {
            if (verbose) {
              System.err.println("Failed to bind argument " + i + " of " + implClass.getName());
            }
            throw new ServiceLoadException(x, implClass);
          }
        }
      } else {
        try {
          val clazz : Class[AnyRef] = _type match {
            case c : Class[_] => c.asInstanceOf[Class[AnyRef]]
            case pt : ParameterizedType => pt.getRawType().asInstanceOf[Class[AnyRef]]
          }
          arguments(i) = resolveImmediate(clazz);
        } catch {
          case x : ServiceLoadException => {
            if (verbose) {
              System.err.println("Failed to bind argument " + i + " of " + implClass.getName());
            }
            throw new ServiceLoadException(x, implClass);
          }
        }
      }
      i += 1
    }
    injectableClass.newInstance(arguments);
  }

  def resolveExtant[S](serviceClass : Class[S], nonEmpty : Boolean = false) : Seq[S] = {
    var services = scala.collection.mutable.ListBuffer[S]()
    var lastException : ServiceLoadException = null
    try {
      val resources = Thread.currentThread().getContextClassLoader().getResources(PATH_PREFIX + serviceClass.getName());
      while (resources.hasMoreElements()) {
        try {
          services ++= resolveURL(serviceClass, resources.nextElement(), false)
        } catch {
          case x : ServiceLoadException => lastException = x;
        }
      }
    } catch {
      case x : IOException => lastException = new ServiceLoadException(serviceClass, x);
    }
    try {
      val resources = Thread.currentThread().getContextClassLoader().getResources(JSL_PATH_PREFIX + serviceClass.getName());
      while (resources.hasMoreElements()) {
        try {
          services ++= resolveURL(serviceClass, resources.nextElement(), true)
        } catch {
          case x : ServiceLoadException => lastException = x;
        }
      }
    } catch {
      case x : IOException => lastException = new ServiceLoadException(serviceClass, x)
    }
    if (!services.isEmpty || (!nonEmpty && lastException == null)) {
      services.toSeq
    } else if (lastException != null) {
      throw lastException;
    } else {
      if (verbose) {
        System.err.println("Could not load non-empty list of services for " + serviceClass);
      }
      throw new ServiceLoadException(serviceClass, "Could not load non-empty list of services");
    }
  }


  private def resolveURL[S](serviceClass : Class[S], url : URL, independent : Boolean) : Iterator[S] = {
    val reader = scala.io.Source.fromInputStream(url.openStream()).getLines()
    try {
      for(s <- reader if !s.matches("\\s+.*") && s != "") yield {
        val ss0 = s.split(";")(0)
        try {
          val c = Thread.currentThread().getContextClassLoader().loadClass(ss0).asInstanceOf[Class[S]]
          try {
            if (verbose) {
              System.err.println("Binding " + ss0 + " as " + serviceClass.getName());
            }
            resolveSingle(serviceClass, c, independent)
          } catch {
            case x : ServiceLoadException => {
              if (verbose) {
                System.err.println("Service not loaded as " + x.getClass().getName() + ": " + x.getMessage());
              }
              throw x
            }
          }
        } catch {
          case x : ClassNotFoundException => {
            if (verbose) {
              System.err.println("Failed to load class " + ss0 + ": " + x.getMessage());
            }
            throw new ServiceLoadException(serviceClass, x);
          }
        }
      }
    } catch {
      case ex : IOException => {
        if (verbose) {
          System.err.println("Error reading service descriptor " + url.toString() + ": " + ex.getMessage());
        }
        throw new ServiceLoadException(serviceClass, ex);
      }
    }
  }
}

case class ServiceLoadException(clazz : Class[_], message : String, cause : Throwable) extends RuntimeException(message,
  cause) {
    def this(clazz : Class[_]) = this(clazz, null, null)
    def this(clazz : Class[_], message : String) = this(clazz, message, null)
    def this(clazz : Class[_], cause : Throwable) = this(clazz, cause.getMessage(), cause)
    def this(x : ServiceLoadException, notFound : Class[_]) = this(notFound, x.getMessage(), x)
}

