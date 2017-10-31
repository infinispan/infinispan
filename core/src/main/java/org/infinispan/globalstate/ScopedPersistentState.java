package org.infinispan.globalstate;

import java.util.function.BiConsumer;

/**
 * ScopedPersistentState represents state which needs to be persisted, scoped by name (e.g. the cache name).
 * The default implementation of persistent state uses the standard {@link java.util.Properties} format with the
 * additional rule that order is preserved. In order to verify state consistency (e.g. across a cluster) a checksum
 * of the state's data can be computed. State properties prefixed with the '@' character will not be included as part
 * of the checksum computation (e.g. @timestamp)
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface ScopedPersistentState {
   String GLOBAL_SCOPE = "___global";

   /**
    * Returns the name of this persistent state's scope
    */
   String getScope();

   /**
    * Sets a state property. Values will be unicode-escaped when written
    */
   void setProperty(String key, String value);

   /**
    * Retrieves a state property
    */
   String getProperty(String key);

   /**
    * Sets an integer state property.
    */
   void setProperty(String key, int value);

   /**
    * Sets a float state property.
    */
   void setProperty(String format, float f);

   /**
    * Retrieves an integer state property
    */
   int getIntProperty(String key);

   /**
    * Retrieves a float state property
    */
   float getFloatProperty(String key);

   /**
    * Performs the specified action on every entry of the state
    */
   void forEach(BiConsumer<String, String> action);

   /**
    * Returns the checksum of the properties excluding those prefixed with @
    */
   int getChecksum();
}
