name := "SparkMLExample"

version := "1.2"

scalaVersion := "2.12.16"

enablePlugins(AssemblyPlugin)

assembly / mainClass := Some("sparkApp")

libraryDependencies := Seq(
    "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"
  , "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
  , "org.apache.spark" %% "spark-streaming" % "3.2.0" % "provided"
  , "org.apache.spark" %% "spark-streaming-kafka-0-10"  % "3.2.0" % "provided"
  , "org.apache.spark" %% "spark-graphx"  % "3.2.0" % "provided"
  , "org.apache.spark" %% "spark-mllib"  % "3.2.0" % "provided"
  , "io.netty" % "netty-buffer" % "4.1.68.Final" % "provided"
  , "log4j" % "log4j" % "1.2.17" % "provided"
)

// for plotting in scala:
// https://mvnrepository.com/artifact/org.scalanlp/breeze-viz
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.2"  % "provided"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.4"
//libraryDependencies += "org.creativescala" %% "doodle-core" % "0.9.21"


// for testing:
libraryDependencies += "org.scoverage" %% "scalac-scoverage-plugin" % "1.4.0" % Test
//coverageEnabled := true

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"


javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
//  test / fork := true


// config file reader:
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

// For accessing object stores, etc. using stocator i.e. Storage Connector for Apache Spark:
// see: https://github.com/CODAIT/stocator
//libraryDependencies += "com.ibm.stocator" % "stocator" % "1.1.5"

// For including the IBM DB2 Database JDBC driver:
libraryDependencies += "com.ibm.db2.jcc" % "db2jcc" % "db2jcc4"
// https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc

// Un-comment this line for including the Oracle Database JDBC driver:
//libraryDependencies += "com.oracle.database.jdbc" % "ojdbc10" % "19.17.0.0"
// https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc10

// Un-comment this line for including the Microsoft SQL Server Database JDBC driver:
//libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.3.jre11"
// https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc

// Un-comment this line for including the Postgresql Database JDBC driver:
//libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"
// https://mvnrepository.com/artifact/org.postgresql/postgresql

// Un-comment this line for including the MySQL Database JDBC driver:
// MySQL database JDBC driver:
//libraryDependencies += "com.mysql" % "mysql-connector-j" % "8.0.32"


// //for adding folder contains jars to the unmanaged classpath:
//unmanagedBase := file("/usr/local/lib")

// //for adding specific jars:
//Compile / unmanagedJars += Attributed.blank(file("c:\\bin\\lib\\breeze-viz_2.12-1.2.jar"))


scalacOptions ++= Seq(
  //  "-target:jvm-1.11",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)
