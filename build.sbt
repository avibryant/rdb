name := "rdb"

version := "0.1-SNAPSHOT"

organization := "com.etsy"

libraryDependencies += "com.ning" % "compress-lzf" % "0.7.0"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "cascading" % "cascading-hadoop" % "2.0.0"
