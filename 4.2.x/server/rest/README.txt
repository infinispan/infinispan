This is the RESTful server for the Infinispan Data Grid. 
Build this as a war, and access server/infinispan-rest to see details on how to use it.

NOTE: you might want to pass these params to the servlet container: -Dbind.address=<bind_address> -Djava.net.preferIPv4Stack=true
These might be needed for a correct setup of jgroups. E.g. for Tomcat, these can be set like this:
      export JAVA_OPTS="-Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true" before starting container  
