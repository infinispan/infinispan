You can read more about how to build Infinispan using Maven in the documentation: https://docs.jboss.org/author/display/ISPN/Contributing+to+Infinispan#ContributingtoInfinispan-BuildingInfinispan
For convenience you can use the provided maven-settings.xml file which enables all additional repositories required for building Infinispan:

  mvn -s maven-settings.xml clean install
  
or use the provided build.sh or build.bat depending on your platform of choice.

