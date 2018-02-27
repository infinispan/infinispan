${infinispan.brand.name} -- a distributed, transactional, highly scalable data structure
==========================================================================

This is the "minimal" ${infinispan.brand.name} distribution, which contains the 
essentials to get you started.

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
   * lib: this directory contains additional dependencies needed for the demo
   * configs: contains sample configurations
   * docs: contains documentation for the API, for the configuration schemas 
     and for the available JMX beans, attributes and operations
   * demos: contains the GUI demo

Requirements
------------

${infinispan.brand.name} needs a JDK 8 compliant Java virtual machine.

