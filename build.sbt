name := "SparkMLExample"

version := "1.1"

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
  , "com.esotericsoftware" % "kryo-shaded" % "4.0.2" % "provided"
)

// kryo serialisation
libraryDependencies += "com.esotericsoftware" % "kryo" % "5.4.0"

// config file reader:
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

// for logging:
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api-scala
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.19.0"

// For accessing object stores, etc. using stocator i.e. Storage Connector for Apache Spark:
// see: https://github.com/CODAIT/stocator
libraryDependencies += "com.ibm.stocator" % "stocator" % "1.1.5"

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

// for plotting in scala 2.11.x only:
//libraryDependencies += "org.vegas-viz" % "vegas_2.11" % "0.3.9"
//libraryDependencies += "org.vegas-viz" % "vegas-spark_2.11" % "0.3.11"

// //for adding folder contains jars to the unmanaged classpath:
//unmanagedBase := file("/usr/local/lib")
// //for adding specific jars:
//Compile / unmanagedJars += Attributed.blank(file("/usr/local/lib/mysql-connector-j-8.0.32.jar"))


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
