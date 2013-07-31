package object nimrod {
  implicit def stringPimps(s : String) = new {
    def %(args : AnyRef*) = String.format(s,args:_*)
    def ls = new java.io.File(s).list()
  }
  def set(key : String, value : String) {
    System.setProperty(key,value)
  }
  def get(key : String) = System.getProperty(key)
}
