name := "Emotion_sentiment_analysis"

version := "0.1"

scalaVersion := "2.13.1"

//libraryDependencies +=
//  "com.typesafe.akka" %% "akka-actor" % "2.5.26"
libraryDependencies ++= {
  Seq(
    "com.typesafe.akka" %% "akka-actor"                           % "2.6.0",
    "org.jsoup"         % "jsoup"                                 % "1.11.3",
    "commons-validator" % "commons-validator"                     % "1.6"
  )
}
