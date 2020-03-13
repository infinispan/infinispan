package org.infinispan.commons.api;

/**
 * Lifecycle interface that defines the lifecycle of components
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface Lifecycle {

   /**
    * Invoked on component start
    */
   void start();

   /**
    * Invoked on component stop
    */
   void stop();

}
