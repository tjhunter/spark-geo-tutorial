import sbt._
import Keys._
object MyBuild extends Build {
  // ...
  val getJars = TaskKey[Unit]("get-jars")
  val getJarsTask = getJars <<= (target, fullClasspath in Runtime) map { (target, cp) =>
    println("Target path is: "+target)
    println(cp.map(_.data).mkString(":"))
  }
  lazy val project = Project (
    "project",
    file ("."),
    settings = Defaults.defaultSettings ++ Seq(getJarsTask)
  )
}
 
