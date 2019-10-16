name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "2.4.4",
                            "org.scalatest" %% "scalatest" % "3.0.1" % "test"
                            )
