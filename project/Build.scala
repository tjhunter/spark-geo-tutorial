import sbt._
import Keys._
object MyBuild extends Build {
  val getJars = TaskKey[Unit]("get-jars")
  val getJarsTask = getJars <<= (target, fullClasspath in Runtime) map { (target, cp) =>
    println("Target path is: "+target)
    def sparkFilter(f:java.io.File):Boolean = {
      val s = f.toString
      if ((s.contains("spark") && !s.contains("classes")) || s.contains("mesos") || s.contains(".sbt/")) {
        false
      } else {
        true
        //s.contains("ivy") || s.contains("classes")
      }
    }
    println(cp.map(_.data).filter(sparkFilter _).mkString(":"))
  }
  lazy val project = Project (
    "project",
    file ("."),
    settings = Defaults.defaultSettings ++ Seq(getJarsTask)
  )
}
 
