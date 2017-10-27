package org.infinispan.counter.api;

import static org.infinispan.counter.api.CounterConfiguration.builder;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A property style format for {@link CounterConfiguration}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class PropertyFormatter {

   private static final PropertyFormatter INSTANCE = new PropertyFormatter();

   public static PropertyFormatter getInstance() {
      return INSTANCE;
   }

   public Properties format(CounterConfiguration configuration) {
      Properties properties = new Properties();
      PropertyKey.TYPE.setProperty(configuration, properties);
      PropertyKey.STORAGE.setProperty(configuration, properties);
      PropertyKey.INITIAL_VALUE.setProperty(configuration, properties);
      switch (configuration.type()) {
         case UNBOUNDED_STRONG:
            break;
         case BOUNDED_STRONG:
            PropertyKey.UPPER_BOUND.setProperty(configuration, properties);
            PropertyKey.LOWER_BOUND.setProperty(configuration, properties);
            break;
         case WEAK:
            PropertyKey.CONCURRENCY.setProperty(configuration, properties);
            break;
         default:
            throw new IllegalStateException();
      }
      return properties;
   }

   public CounterConfiguration.Builder from(Properties properties) {
      CounterType type = CounterType.valueOf(properties.getProperty(PropertyKey.TYPE.key));
      CounterConfiguration.Builder builder = builder(type);
      fromProperty(properties, PropertyKey.STORAGE, Storage::valueOf, builder::storage);
      fromProperty(properties, PropertyKey.INITIAL_VALUE, Long::valueOf, builder::initialValue);
      switch (type) {
         case WEAK:
            fromProperty(properties, PropertyKey.CONCURRENCY, Integer::valueOf, builder::concurrencyLevel);
            break;
         case BOUNDED_STRONG:
            fromProperty(properties, PropertyKey.UPPER_BOUND, Long::valueOf, builder::upperBound);
            fromProperty(properties, PropertyKey.LOWER_BOUND, Long::valueOf, builder::lowerBound);
            break;
         case UNBOUNDED_STRONG:
            break;
         default:
            throw new IllegalStateException();
      }
      return builder;
   }

   private <T> void fromProperty(Properties properties, PropertyKey key, Function<String, T> read, Consumer<T> setter) {
      String value = properties.getProperty(key.key);
      if (value != null) {
         setter.accept(read.apply(value));
      }
   }

   private enum PropertyKey {
      TYPE("type") {
         @Override
         void setProperty(CounterConfiguration config, Properties properties) {
            properties.setProperty(key, String.valueOf(config.type()));
         }
      },
      INITIAL_VALUE("initial-value") {
         @Override
         void setProperty(CounterConfiguration config, Properties properties) {
            properties.setProperty(key, String.valueOf(config.initialValue()));
         }
      },
      STORAGE("storage") {
         @Override
         void setProperty(CounterConfiguration config, Properties properties) {
            properties.setProperty(key, String.valueOf(config.storage()));
         }
      },
      UPPER_BOUND("upper-bound") {
         @Override
         void setProperty(CounterConfiguration config, Properties properties) {
            properties.setProperty(key, String.valueOf(config.upperBound()));
         }
      },
      LOWER_BOUND("lower-bound") {
         @Override
         void setProperty(CounterConfiguration config, Properties properties) {
            properties.setProperty(key, String.valueOf(config.lowerBound()));
         }
      },
      CONCURRENCY("concurrency-level") {
         @Override
         void setProperty(CounterConfiguration config, Properties properties) {
            properties.setProperty(key, String.valueOf(config.concurrencyLevel()));
         }
      };
      final String key;

      PropertyKey(String key) {
         this.key = key;
      }

      abstract void setProperty(CounterConfiguration config, Properties properties);

   }

}
