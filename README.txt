====================================================
=	Steps to run the YADA Database Application =
====================================================

1. Unzip the folder "YADA.zip".

2. Add the below jars to the current working library
	postgresql-9.1-902.jdbc4
	weka.jar

3. Copy the file "DatabaseUtils.props" and add under the 
   referenced library.

4. Copy the below source codes and add to working directory
	YADAApplication.java
	DataSetAnalysis.java
	CRUD.java

5. Compile the java files using the below command if not 
   working on Eclipse API.
	- javac  YADAApplication.java
	  javac	 DataSetAnalysis.java
	  javac  CRUD.java

6. To run the application run the file YADAApplication.java
   [If not running using API , use the command java YADAApplication]