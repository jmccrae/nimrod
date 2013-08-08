package nimrod

import java.io._
import java.util.ArrayList
import java.util.zip.{GZIPInputStream,GZIPOutputStream}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream,BZip2CompressorOutputStream}
import collection.JavaConversions._

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
     * Call this after calling all CLIOpts to verify the CLIOpts are valid
     *
     * @param scriptName The name of this script
     * @return {@code true} if the CLIOpts are valid
     */
    def verify(implicit workflow : Workflow) : Boolean = {
      val message = new StringBuilder()
      val ln = System.getProperty("line.separator")
        if (!succeeded || !_args.isEmpty) {
            if (!_args.isEmpty) {
                message.append("Too many arguments"+ln);
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

    def outFileOrStdout() : PrintStream = {
        val arg = new Argument("out", null, "The out file or nothing to use STDOUT", true);
        argObjs.add(arg);
        if (_args.isEmpty()) {
            return System.out;
        } else {
            val file = new File(_args.get(0));
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

    def doubleValue(name : String, defaultValue : Double, description : String) : Double = {
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

    def enumOptional[T <: Enumeration](name : String, enum : T, defaultValue : T#Value, description : String) : T#Value  = {
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

    //@SuppressWarnings("unchecked")
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

    //@SuppressWarnings("unchecked")
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

    def string(name : String, defaultValue : String, description : String) : String = {
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

    private class Argument(val name : String, val flag : String, val description : String, val optional : Boolean) {
        var message = ""
    }
}
