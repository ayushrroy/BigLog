# How to set up IntelliJ with Scala!

For setting up Scala in IntelliJ you will just need to download IntelliJ in your machine. This version would be running without Hadoop. 


## Steps

 1. Download the latest version of IntelliJ
 2. When the project menu appears, go to Configure at the bottom right and click on the Plugins  option available in the drop-down, as shown here:
![](https://packt-type-cloud.s3.amazonaws.com/uploads/sites/2445/2018/04/378c732f-369e-46d0-b135-a84c8356bbce.png)
 3. Search for the Scala Plug in, download it and restart IntelliJ. 
 4. Go to create New Project, choose a suitable name and the following settings (check Config.png) (If you do not have the right JDK you can use the drop down and click download JDK in order to install the right JDK Java 1.8)
 5. Wait for the project to finish setting up, this usually takes a few minutes. 
 6. Go to a file called build.sbt and enter this line:
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
 7. The Scala set up should be complete, you can try running a sample Scala program to check if you get the right value. (firstdemo.scala)