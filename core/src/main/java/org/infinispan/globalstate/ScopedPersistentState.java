package org.infinispan.globalstate;

import java.util.function.BiConsumer;

/**
 * ScopedPersistentState.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public interface ScopedPersistentState {
   public static final String GLOBAL_SCOPE = "___global";

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
    * Performs the specified action on every entry of the state
    */
   void forEach(BiConsumer<String, String> action);
}
