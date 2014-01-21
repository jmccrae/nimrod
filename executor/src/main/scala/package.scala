import java.io.File

package object nimrod {
  implicit def stringPimps(s : String) = new {
    /**
     * @deprecated Use built-in format instead
     */
    @Deprecated
    def %(args : AnyRef*) = String.format(s,args:_*)
    def ls = new java.io.File(s).list()
  }
  implicit def filePimps(f : File) = new {
    def ls = f.list()
    def path = f.getCanonicalPath()
  }
  def set(key : String, value : String) {
    System.setProperty(key,value)
  }
  def get(key : String) = System.getProperty(key)
  def pwd = System.getProperty("user.dir")
}
