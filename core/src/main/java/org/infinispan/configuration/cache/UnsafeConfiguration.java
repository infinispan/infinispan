package org.infinispan.configuration.cache;

/**

 * Controls certain tuning parameters that may break some of Infinispan's public API contracts in exchange for better
 * performance in some cases.
 * <p />
 * Use with care, only after thoroughly reading and understanding the documentation about a specific feature.
 * <p />
 * @see UnsafeConfigurationBuilder
 */
public class UnsafeConfiguration {

   private final boolean unreliableReturnValues;

   UnsafeConfiguration(boolean unreliableReturnValues) {
      this.unreliableReturnValues = unreliableReturnValues;
   }

   /**
    * Specifies whether Infinispan is allowed to disregard the {@link Map} contract when providing return values for
    * {@link org.infinispan.Cache#put(Object, Object)} and {@link org.infinispan.Cache#remove(Object)} methods.
    */
   public boolean unreliableReturnValues() {
      return unreliableReturnValues;
   }
}
