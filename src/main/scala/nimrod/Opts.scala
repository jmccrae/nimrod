package nimrod

import java.io._
import java.util.ArrayList
import java.util.zip.{GZIPInputStream,GZIPOutputStream}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream,BZip2CompressorOutputStream}
import collection.JavaConversions._

/**
 * The Opts class represents the arguments to be passed to the Nimrod script.
 * A single instance called {@code opts} is injected at the beginning of each script.
 * Note it is important to call {@code opts.verify} after having declared all argument
 * to check that there are no extra arguments and print the usage message if the
 * user gives an incorrect command line
 */
class Opts(args : Seq[String]) {
    private val argObjs = new ArrayList[Argument]()
    private var requireFilesExist = true
    private var succeeded = true;
    private val _args = collection.mutable.ListBuffer[String](args:_*)

    def doNotRequireFileExists = {
      requireFilesExist = false
      this
    }

    /**
     * Call this after calling all opts to verify the opts are valid
     *
     * @param scriptName The name of this script
     * @return {@code true} if the opts are valid
     */
    def verify(implicit workflow : Workflow) : Boolean = {
      val message = new StringBuilder()
      val ln = System.getProperty("line.separator")
        if (!succeeded || !_args.isEmpty) {
            if (!_args.isEmpty) {
                message.append("Too many arguments: " + _args.mkString(",") +ln);
            }
            for (argObj <-  argObjs) {
                if (argObj.message != null && argObj.message != "") {
                    message.append(argObj.message + ln);
                }
            }
            message.append("\nUsage:\n"
                    + "\tnimrod " + workflow.name + " ");
            for (i <- 0 until argObjs.size()) {
                if (argObjs.get(i).optional) {
                    message.append("[");
                }
                if (argObjs.get(i).flag != null) {
                    message.append("-" + argObjs.get(i).flag + " ");
                }
                if (argObjs.get(i).name != null) {
                    message.append(argObjs.get(i).name);
                }
                if (argObjs.get(i).optional) {
                    message.append("]");
                }
                if (i + 1 != argObjs.size()) {
                    message.append(" ");
                }
            }
            message.append("\"" + ln);
            for (argObj <- argObjs) {
                message.append("\t  * " + (if(argObj.name == null) { argObj.flag } else { argObj.name }) + ": " + argObj.description + ln);
            }
            workflow.compileFail(message.toString)
            return false;
        } else {
            return true;
        }
    }

    /**
     * Return a required argument that is a file for reading only
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     */
    def roFile(name : String, description : String) : File = {
        val arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return null;
        } else {
            val file = new File(_args.get(0));
            if (requireFilesExist && (!file.exists() || !file.canRead())) {
                arg.message = "Cannot access [" + file.getPath() + "] for " + name;
                succeeded = false;
                _args.remove(0);
                return null;
            }
            _args.remove(0);
            return file;
        }
    }

    /**
     * Return a non-required argument that is a file for reading only
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def roFile(name : String, description : String, defaultValue : File) : File = {
        val arg = new Argument(name, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return defaultValue;
        } else {
            for (i <- 0 until _args.size() - 1) {
                if (_args.get(i).equals("-" + name)) {
                    val file = new File(_args.get(i + 1));
                    if (requireFilesExist && (!file.exists() || !file.canRead())) {
                        arg.message = "Cannot access [" + file.getPath() + "] for " + name;
                        succeeded = false;
                        _args.remove(0);
                        return null;
                    }
                    _args.remove(i);
                    _args.remove(i);
                    return file;
                }
            }
            return defaultValue;
        }
    }

    /**
     * Return an argument that is a file for output only
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     */
    def woFile(name : String, description : String) : File = {
        val arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return null;
        } else {
            val file = new File(_args.get(0));
            if (requireFilesExist && file.exists() && !file.canWrite()) {
                arg.message = "Cannot access [" + file.getPath() + "] for " + name;
                succeeded = false;
                _args.remove(0);
                return null;
            }
            _args.remove(0);
            return file;
        }
    }

    /**
     * Return a non-requried argument that is a file for output only
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def woFile(name : String, description : String, defaultValue : File) : File = {
        val arg = new Argument(name, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return defaultValue;
        } else {
            for (i <- 0 until _args.size() - 1) {
                if (_args.get(i).equals("-" + name)) {
                    val file = new File(_args.get(i + 1));
                    if (file.exists() && requireFilesExist && !file.canWrite()) {
                        arg.message = "Cannot access [" + file.getPath() + "] for " + name;
                        succeeded = false;
                        _args.remove(i);
                        _args.remove(i);
                        return null;
                    }
                    _args.remove(i);
                    _args.remove(i);
                    return file;
                }
            }
            return defaultValue;
        }
    }

    /**
     * Return an argument that if specified is a file to write to or otherwise is STDOUT
     */
    def outFileOrStdout() : PrintStream = {
        val arg = new Argument("out", null, "The out file or nothing to use STDOUT", true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return System.out;
        } else {
            val file = new File(_args.get(0));
            System.err.println("File: " + file.getPath())
            if (file.exists() && requireFilesExist && !file.canWrite()) {
                arg.message = "Cannot access [" + file.getPath() + "] for out";
                succeeded = false;
                _args.remove(0);
                return null;
            }
            _args.remove(0);
            try {
                return new PrintStream(file);
            } catch {
              case x : FileNotFoundException => {
                return null;
              }
            }
        }
    }

    /**
     * Return a required argument that is an integer
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     */
    def intValue(name : String, description : String) : Int = {
        val arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return 0;
        } else {
            val i = try {
                Integer.parseInt(_args.get(0));
            } catch {
              case x : NumberFormatException => {
                arg.message = "Not an integer " + _args.get(0) + " for " + name;
                succeeded = false;
                _args.remove(0);
                return 0;
              }
            }
            _args.remove(0);
            return i;
        }
    }

    /**
     * Return a required argument that is an integer value
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def intValue(name : String, description : String, defaultValue : Int) : Int = {
        val arg = new Argument(name, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return defaultValue;
        } else {
            for (i <- 0 until _args.size()) {
                try {
                    if (_args.get(i).equals("-" + name)) {
                        val j = Integer.parseInt(_args.get(i + 1));
                        _args.remove(i);
                        _args.remove(i);
                        return j;
                    }
                } catch {
                  case x : NumberFormatException => {
                    arg.message = "Not an integer " + _args.get(0) + " for " + name;
                    succeeded = false;
                    _args.remove(i);
                    _args.remove(i);
                    return 0;
                  }
                }
            }
            return defaultValue;
        }

    }

    /**
     * Return a required argument that is a non-negative integer
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     */
    def nonNegIntValue(name : String, description : String) : Int = {
        val arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return 0;
        } else {
            val i = try {
                Integer.parseInt(_args.get(0));
            } catch {
              case x : NumberFormatException => {
                arg.message = "Not an integer " + _args.get(0) + " for " + name;
                succeeded = false;
                _args.remove(0);
                return 0;
              }
            }
            if (i <= 0) {
                arg.message = name + " must be greater than 0";
                succeeded = false;
            }
            _args.remove(0);
            return i;
        }
    }

    /**
     * Return a required argument that is a double value
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     */
    def doubleValue(name : String, description : String) : Double = {
        val arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return 0;
        } else {
            val i = try {
                (_args.get(0)).toDouble;
            } catch { 
              case x : NumberFormatException => {
                arg.message = "Not a number " + _args.get(0) + " for " + name;
                succeeded = false;
                _args.remove(0);
                return 0;
              }
            }
            _args.remove(0);
            return i;
        }
    }

    /**
     * Return a non-required argument, which is a floating point vaue
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def doubleValue(name : String, description : String, defaultValue : Double) : Double = {
        val arg = new Argument(name, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return defaultValue;
        } else {
            for (i <- 0 until _args.size() - 1) {
                if (_args.get(i).equals("-" + name)) {
                    val d = try {
                        (_args.get(i + 1)).toDouble;
                    } catch {
                      case x : NumberFormatException => {
                        arg.message = "Not a number " + _args.get(0) + " for " + name;
                        succeeded = false;
                        _args.remove(i);
                        _args.remove(i);
                        return 0;
                      }
                    }
                    _args.remove(i);
                    _args.remove(i);
                    return d;
                }
            }
            return defaultValue;
        }
    }

    /**
     * Return a non-required value which is a string of one possible value
     * @param name The symbolic name for this argument
     * @param enum The enumeration of possible values
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def enumOptional[T <: Enumeration](name : String, enum : T, description : String, defaultValue : T#Value) : T#Value  = {
        val arg = new Argument(name, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return defaultValue;
        } else {
            for (i <- 0 until _args.size() - 1) {
                if (_args.get(i).equals("-" + name)) {
                    try {
                        val t = enum.withName(_args.get(i + 1));
                        _args.remove(i);
                        _args.remove(i);
                        return t;
                    } catch {
                      case x : IllegalArgumentException => {
                        arg.message = "Invalid argument for " + name + ":" + _args.get(i + 1);
                        succeeded = false;
                        _args.remove(i);
                        _args.remove(i);
                        return defaultValue;
                      }
                    }
                }
            }
            return defaultValue;
        }
    }

    /**
     * Return true if the flag is set
     * @param name The symbolic name for this argument (the flag)
     * @param description The description of this argument
     */
    def flag(name : String, description : String) : Boolean = {
        val arg = new Argument(null, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return false;
        } else {
            for (i <- 0 until _args.size()) {
                if (_args.get(i).equals("-" + name)) {
                    _args.remove(i);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Return an argument that refers to a class on the class path
     * @param name The symbolic name for this argument
     * @param interfase The interface that the class is expected to implement
     * @param description The description of this argument
     */
    def clazz(name : String, interfase : Class[_], description : String) : Class[_]  = {
        val arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return null;
        } else {
            val clazz = try {
                Class.forName(_args.get(0));
            } catch {
              case x : ClassNotFoundException => {
                arg.message = "Class not found: " + _args.get(0);
                succeeded = false;
                _args.remove(0);
                return null;
              }
            }
            if (!interfase.isAssignableFrom(clazz)) {
                arg.message = clazz.getName() + " must implement " + interfase.getName();
                succeeded = false;
                _args.remove(0);
                return null;
            }
            _args.remove(0);
            return clazz;
        }
    }

    /**
     * Return an argument that refers to a class on the classpath, with some pregiven aliases
     * @param name The symbolic name for this argument
     * @param interfase The interface the class is expected to implement
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def clazz(name : String, interfase : Class[_], description : String, shortNames : Map[String,String]) : Class[_] = {
        val sb = new StringBuilder(" (");
        for (s <- shortNames.keySet) {
            sb.append(s);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        val arg = new Argument(name, null, description + sb.toString(), false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return null;
        } else {
            val className : String = if(shortNames.containsKey(_args.get(0))) { shortNames(_args.get(0)) } else { _args.get(0) };
            val clazz = try {
                Class.forName(className);
            } catch {
              case x : ClassNotFoundException => {
                arg.message = "Class not found: " + _args.get(0);
                succeeded = false;
                _args.remove(0);
                return null;
              }
            }
            if (!interfase.isAssignableFrom(clazz)) {
                arg.message = clazz.getName() + " must implement " + interfase.getName();
                succeeded = false;
                _args.remove(0);
                return null;
            }
            _args.remove(0);
            return clazz;
        }
    }

    /**
     * Return an argument that represents a single string
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     */
    def string(name : String, description : String) : String = {
      val arg = new Argument(name,null,description,false)
      argObjs.add(arg)
      if(_args.isEmpty()) {
        arg.message = "Too few arguments: expected " + name;
        succeeded = false;
        return null;
      } else {
        val str = _args.get(0)
        _args.remove(0);
        return str
      }
    }

    /**
     * Return a non-requried argument that represents a single string
     * @param name The symbolic name for this argument
     * @param description The description of this argument
     * @param defaultValue The default value to use
     */
    def string(name : String, description : String, defaultValue : String) : String = {
        val arg = new Argument(name, name, description, true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return defaultValue;
        } else {
            for (i <- 0 until _args.size() - 1) {
                if (_args.get(i).equals("-" + name)) {
                  val str = _args.get(i+1)
                  _args.remove(i);
                  _args.remove(i);
                  return str;
                }
            }
            return defaultValue;
        }
    }

    def listOfArgs(name : String,description : String) : List[String] = {
      val arg = new Argument(name + "...",null,description,false)
      argObjs.add(arg)
      val list = _args.toList
      _args.clear()
      return list
    }
/*    public Language language(String name, String description) {

        final Argument arg = new Argument(name, null, description, false);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            arg.message = "Too few arguments: expected " + name;
            succeeded = false;
            return null;
        } else {
            final Language language;
            try {
                language = Language.get(_args.get(0));
            } catch (LanguageCodeFormatException x) {
                arg.message = "Bad language code: " + _args.get(0);
                succeeded = false;
                _args.remove(0);
                return null;
            }
            _args.remove(0);
            return language;
        }
    }*/

   /**
    * Treat all remaining arguments as system properties of the form "property=value"
    */
    def restAsSystemProperties() {
        val arg = new Argument("...", null, "Other parameters as x=y", false);
        for (argi <- _args) {
            val s = argi.split("=");
            if (s.length != 2) {
                arg.message = "Not a valid argument: " + argi;
                succeeded = false;
                return;
            } else {
                System.setProperty(s(0), s(1));
            }
        }
        _args.clear();
    }

    /**
     * Return a file as an input stream, that unzips if the file ends in .gz or
     * .bz2.
     *
     * @param file The file
     * @return File as an input stream
     * @throws IOException If the file is not found or is not a correct zipped
     * file or some other reason
     */
    def openInput(file : File) : io.Source = {
        if (file.getName().endsWith(".gz")) {
            io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(file)))
        } else if (file.getName().endsWith(".bz2")) {
            io.Source.fromInputStream(new BZip2CompressorInputStream(new FileInputStream(file)))
        } else {
            io.Source.fromFile(file)
        }
    }

    /**
     * Return a file as an input stream, that unzips if the file ends in .gz or
     * .bz2.
     *
     * @param file The file
     * @return File as an input stream
     * @throws IOException If the file is not found or is not a correct zipped
     * file or some other reason
     */
    def openInput(file : String) : io.Source = openInput(new File(file))


    /**
     * Return a file as an output stream, that zips if the file ends in .gz or
     * .bz2.
     *
     * @param file The file
     * @return File as an output stream
     * @throws IOException If the file is not found or some other reason
     */
    def openOutput(file : File) : PrintStream = {
        if (file.getName().endsWith(".gz")) {
            new PrintStream(new GZIPOutputStream(new FileOutputStream(file)))
        } else if (file.getName().endsWith(".bz2")) {
            new PrintStream(new BZip2CompressorOutputStream(new FileOutputStream(file)))
        } else {
            new PrintStream(file)
        }
    }

    /**
     * Return a file as an output stream, that zips if the file ends in .gz or
     * .bz2.
     *
     * @param file The file
     * @return File as an output stream
     * @throws IOException If the file is not found or some other reason
     */
    def openOutput(file : String) : PrintStream = openOutput(new File(file))
    
    private class Argument(val name : String, val flag : String, val description : String, val optional : Boolean) {
        var message = ""
    }
}
