package nimrod.tasks

import nimrod._
import java.io.File

object maven {
  def apply(target : String, pom : String = null, args : Map[String,String] = Map())(implicit workflow : Workflow) = {
    val pomFile = new File(if(pom != null) {
      pom
    } else {
      System.getProperty("user.dir"+"/pom.xml")
    })
    Do("mvn",("-f" :: pomFile.getCanonicalPath :: target ::
      args.map({
        case (k,v) => "-D"+k+"=\""+v+"\""
      }).toList):_*
    ).dir(pomFile.getParentFile())
  }
}

object mj {
  def apply(clazz : String, args : List[String] = Nil, pom : String = null)(implicit workflow : Workflow) = {
    val pomFile = new File(if(pom != null) {
      pom
    } else {
      System.getProperty("user.dir"+"/pom.xml")
    })
    Do("mvn","-q","-f",pomFile.getCanonicalPath,"exec:java",
      "-Dexec.mainClass=" + clazz,
      "-Dexec.args=" + args.mkString(" ")
    ).dir(pomFile.getParentFile())
  }
}

