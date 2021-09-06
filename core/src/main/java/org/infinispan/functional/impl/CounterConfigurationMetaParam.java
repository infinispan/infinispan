package org.infinispan.functional.impl;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.functional.MetaParam;

/**
 * Stores the {@link CounterConfiguration}.
 * <p>
 * The metadata is static and doesn't change. It is sent when initializing a counter and it is kept locally in all the
 * nodes. This avoids transfer information about the counter in every operation (e.g. boundaries/reset).
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class CounterConfigurationMetaParam implements MetaParam.Writable<CounterConfiguration> {

   private final CounterConfiguration configuration;

   public CounterConfigurationMetaParam(CounterConfiguration configuration) {
      this.configuration = configuration;
   }


   @Override
   public CounterConfiguration get() {
      return configuration;
   }

   @Override
   public String toString() {
      return "CounterConfigurationMetaParam{" +
            "configuration=" + configuration +
            '}';
   }
}
