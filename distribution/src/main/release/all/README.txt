${infinispan.brand.name} -- a distributed, transactional, highly scalable data structure
==========================================================================

This is the "all" ${infinispan.brand.name} distribution, which contains the basic library
and all additional optional modules.

   * infinispan-embedded.jar: this is the main jar which is all you need
     for setting up either a local or a clustered cache. If you are running
     ${infinispan.brand.name} in a plain JavaSE environment or in a basic servlet 
     container (such as Tomcat) you will also need to add the Java 
     Transaction API jar (which you can find in the lib directory)
     This jar also includes support for the following additional features:
      - JCache (you will need the cache-api.jar)
      - CDI (you will need a CDI container)
      - SingleFile, JDBC, LevelDB and JPA cache stores (for the latter you
        will need to add Hibernate to your dependencies).
   * infinispan-embedded-query.jar: add this jar to your application if
     you need query capabilities
   * infinispan-cli.jar: provides the command-line interface functionality
     (you can use "bin/ispn-cli.sh" to launch it). 
   * modules: this directory contains additional optional modules. Some of 
     these modules require external dependencies which are listed in a
     runtime-classpath.txt file alongside each module's jar.
   * lib: this directory contains additional dependencies needed for the demos
     and for the modules
   * configs: contains sample configurations
   * docs: contains documentation for the API, for the configuration schemas 
     and for the available JMX beans, attributes and operations
   * demos: contains demos showcasing ${infinispan.brand.name}'s capabilities.

Requirements
------------

${infinispan.brand.name} needs a JDK 8 compliant Java virtual machine.

