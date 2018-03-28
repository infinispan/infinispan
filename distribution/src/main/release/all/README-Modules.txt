Infinispan is distributed as a set of core components and a number of optional 
modules which include additional functionality.

To use the core functionality of Infinispan, just add 'infinispan-embedded.jar' 
to your application. If you additionally require querying functionality also add
'infinispan-embedded-query.jar'.
The optional modules are located under the 'modules' directory. If you wish to 
use one or more of these modules, you will also need the module's jar file and 
all of its dependencies (listed in the corresponding runtime-classpath.txt file)
to be on your classpath.

NOTE: If you use a dependency management tool such as Maven, Gradle or Ivy, you
should not use the jars provided in this archive but rather add the appropriate
dependencies to your build file. See the Getting Started Guide available at
 http://infinispan.org/docs/stable/getting_started/getting_started.html
for instructions on how to do that.

