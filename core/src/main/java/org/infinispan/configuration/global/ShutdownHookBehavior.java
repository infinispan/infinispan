package org.infinispan.configuration.global;

/**
 * Behavior of the JVM shutdown hook registered by the cache
 */
public enum ShutdownHookBehavior {
   /**
    * By default a shutdown hook is registered if no MBean server (apart from the JDK default) is detected.
    */
   DEFAULT,
   /**
    * Forces the cache to register a shutdown hook even if an MBean server is detected.
    */
   REGISTER,
   /**
    * Forces the cache NOT to register a shutdown hook, even if no MBean server is detected.
    */
   DONT_REGISTER;
}