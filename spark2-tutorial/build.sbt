name := "spark2-tutorial"

version := "1.0"
//Older Scala Version
scalaVersion := "2.11.8"

val overrideScalaVersion = "2.11.8"
val sparkVersion = "2.2.0"
//val sparkVersion = "1.6.0"

val sparkXMLVersion = "0.3.4"
val sparkCsvVersion = "1.5.0"
val sparkElasticVersion = "2.3.4"
val sscKafkaVersion = "1.6.2"
val sparkMongoVersion = "1.0.0"
val sparkCassandraVersion = "1.6.0"

//Override Scala Version to the above 2.11.8 version
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"      %%  "spark-core"      %   sparkVersion  exclude("jline", "2.12"),
  "org.apache.spark"      %% "spark-sql"        % sparkVersion excludeAll(ExclusionRule(organization = "jline"),ExclusionRule("name","2.12")),
    "org.apache.spark"      %% "spark-hive"       % sparkVersion,
  "org.apache.spark"      %% "spark-yarn"       % sparkVersion,
  "org.apache.spark"      %% "spark-mllib"      % sparkVersion,
  "com.databricks"        %% "spark-xml"        % sparkXMLVersion,
  "com.databricks"        %% "spark-csv"        % sparkCsvVersion   withSources(),
  "org.apache.spark"      %% "spark-graphx"     % sparkVersion,
  "org.apache.spark"      %% "spark-catalyst"   % sparkVersion,
  "org.apache.spark"      %% "spark-streaming"  % sparkVersion,
  //  "com.101tec"           % "zkclient"         % "0.9",
  "org.elasticsearch"     %% "elasticsearch-spark"        %     sparkElasticVersion


//  "org.apache.spark"      %% "spark-streaming-kafka"     % sscKafkaVersion,
//  "org.mongodb.spark"      % "mongo-spark-connector_2.11" %  sparkMongoVersion,
//  "com.stratio.datasource" % "spark-mongodb_2.10"         % "0.11.1"

  // Adding this directly as part of Build.sbt throws Guava Version incompatability issues.
  // Please look my Spark Cassandra Guava Shade Project and use that Jar directly.
  //"com.datastax.spark"     % "spark-cassandra-connector_2.11" % sparkCassandraVersion
)

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.0.0" % "provided"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.3" % "test"

