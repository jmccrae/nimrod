package object nimrod {
  implicit def stringPimps(s : String) = new {
    def %(args : AnyRef*) = String.format(s,args:_*)
  }
}
