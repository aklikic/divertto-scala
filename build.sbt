import sbt.Keys._

name := "divertto-scala"
organization := "com.lightbend.akka"
version := "1.0.0"
scalaVersion := Dependencies.scalaVer
libraryDependencies ++= Dependencies.dependencies

fork in run := true
connectInput in run := true
