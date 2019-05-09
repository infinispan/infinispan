package org.infinispan.server.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ServerTestConfiguration {
   String configurationFile();

   /**
    * Defines the number of servers to start.
    */
   int numServers() default 2;

   /**
    * Defines how the server instances should be started.
    */
   ServerRunMode runMode() default ServerRunMode.EMBEDDED;
}
