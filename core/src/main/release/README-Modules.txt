Infinispan is distributed as a set of core components and a number of optional 
modules which include additional functionality.
To use the core functionality of Infinispan, just add 'infinispan-embedded.jar' 
to your application. If you additionally require querying functionality also add
'infinispan-embedded-query.jar'.
The optional modules are located under the 'modules' directory. If you wish to 
use one or more of these modules, you will also need the module's jar file and 
all of its dependencies (listed in the corresponding runtime-classpath.txt file)
to be on your classpath.
