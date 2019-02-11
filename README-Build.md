You can read more about how to build Infinispan in the documentation:

http://infinispan.org/docs/dev/contributing/contributing.html

Provided you already have the correct versions of Java and Maven installed, you can get started right away.
For convenience you can use the provided maven-settings.xml file which enables all additional repositories required for
building Infinispan:

  mvn -s maven-settings.xml clean install

or use the provided build.sh or build.bat depending on your platform of choice.

