package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Controls certain tuning parameters that may break some of Infinispan's public API contracts in exchange for better
 * performance in some cases.
 * <p />
 * Use with care, only after thoroughly reading and understanding the documentation about a specific feature.
 * <p />
 */
public class UnsafeConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<UnsafeConfiguration> {

   private boolean unreliableReturnValues = false;

   protected UnsafeConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Specify whether Infinispan is allowed to disregard the {@link Map} contract when providing return values for
    * {@link org.infinispan.Cache#put(Object, Object)} and {@link org.infinispan.Cache#remove(Object)} methods.
    * <p />
    * Providing return values can be expensive as they may entail a read from disk or across a network, and if the usage
    * of these methods never make use of these return values, allowing unreliable return values helps Infinispan
    * optimize away these remote calls or disk reads.
    * <p />
    * @param allowUnreliableReturnValues if true, return values for the methods described above should not be relied on.
    */
   public UnsafeConfigurationBuilder unreliableReturnValues(boolean allowUnreliableReturnValues) {
      this.unreliableReturnValues = allowUnreliableReturnValues;
      return this;
   }

   @Override
   public void validate() {
      // Nothing to validate
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public UnsafeConfiguration create() {
      return new UnsafeConfiguration(unreliableReturnValues);
   }

   @Override
   public UnsafeConfigurationBuilder read(UnsafeConfiguration template) {
      this.unreliableReturnValues = template.unreliableReturnValues();

      return this;
   }

   @Override
   public String toString() {
      return "UnsafeConfigurationBuilder{" +
            "unreliableReturnValues=" + unreliableReturnValues +
            '}';
   }
}
