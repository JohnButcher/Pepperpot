name := "PepperPot"
 
version := "1.0"
 
scalaVersion := "2.10.4"

val sparkVersion = "1.5.0"
 
libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
   "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
   "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

)

   // ,"com.databricks" % "spark-csv_2.10" % "1.2.0"

