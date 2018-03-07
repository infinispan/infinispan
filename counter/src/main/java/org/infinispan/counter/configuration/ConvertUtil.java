package org.infinispan.counter.configuration;

import static org.infinispan.counter.api.CounterConfiguration.builder;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;

/**
 * Utility methods to convert {@link CounterConfiguration} to and from {@link StrongCounterConfiguration} and {@link WeakCounterConfiguration}
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class ConvertUtil {

   private ConvertUtil() {
   }

   public static CounterConfiguration parsedConfigToConfig(AbstractCounterConfiguration configuration) {
      if (configuration instanceof StrongCounterConfiguration) {
         return parsedConfigToConfig((StrongCounterConfiguration) configuration);
      } else if (configuration instanceof WeakCounterConfiguration) {
         return parsedConfigToConfig((WeakCounterConfiguration) configuration);
      }
      throw new IllegalStateException(
            "[should never happen] unknown CounterConfiguration class: " + configuration.getClass());
   }

   public static AbstractCounterConfiguration configToParsedConfig(String name,
         CounterConfiguration configuration) {
      switch (configuration.type()) {
         case WEAK:
            WeakCounterConfigurationBuilder wBuilder = new WeakCounterConfigurationBuilder(null);
            wBuilder.concurrencyLevel(configuration.concurrencyLevel());
            populateCommonAttributes(wBuilder, name, configuration);
            return wBuilder.create();
         case BOUNDED_STRONG:
            StrongCounterConfigurationBuilder bBuilder = new StrongCounterConfigurationBuilder(null);
            bBuilder.upperBound(configuration.upperBound());
            bBuilder.lowerBound(configuration.lowerBound());
            populateCommonAttributes(bBuilder, name, configuration);
            return bBuilder.create();
         case UNBOUNDED_STRONG:
            StrongCounterConfigurationBuilder uBuilder = new StrongCounterConfigurationBuilder(null);
            populateCommonAttributes(uBuilder, name, configuration);
            return uBuilder.create();
         default:
            throw new IllegalStateException(
                  "[should never happen] unknown CounterConfiguration class: " + configuration.getClass());
      }
   }


   private static void populateCommonAttributes(AbstractCounterConfigurationBuilder<?, ?> builder,
         String name, CounterConfiguration config) {
      builder.name(name);
      builder.initialValue(config.initialValue());
      builder.storage(config.storage());
   }

   private static CounterConfiguration parsedConfigToConfig(StrongCounterConfiguration configuration) {
      return configuration.isBound() ?
            builder(CounterType.BOUNDED_STRONG)
                  .initialValue(configuration.initialValue())
                  .lowerBound(configuration.lowerBound())
                  .upperBound(configuration.upperBound())
                  .storage(configuration.storage())
                  .build() :
            builder(CounterType.UNBOUNDED_STRONG)
                  .initialValue(configuration.initialValue())
                  .storage(configuration.storage())
                  .build();
   }

   private static CounterConfiguration parsedConfigToConfig(WeakCounterConfiguration configuration) {
      return builder(CounterType.WEAK)
            .initialValue(configuration.initialValue())
            .storage(configuration.storage())
            .concurrencyLevel(configuration.concurrencyLevel())
            .build();
   }

}
