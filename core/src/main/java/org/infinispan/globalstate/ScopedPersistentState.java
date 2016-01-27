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
}
