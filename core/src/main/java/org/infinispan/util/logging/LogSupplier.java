package org.infinispan.util.logging;

/**
 * Provides a {@link Log} instance to use.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public interface LogSupplier {

   /**
    * @return {@code true} if "TRACE" is enabled in this {@link Log} instance, {@code false} otherwise.
    */
   boolean isTraceEnabled();

   /**
    * @return The {@link Log} instance.
    */
   Log getLog();
}
