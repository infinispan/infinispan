package org.infinispan.counter.impl.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.functional.MetaParam;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;

/**
 * Stores the {@link CounterConfiguration} with the {@link org.infinispan.counter.impl.entries.CounterValue}.
 * <p>
 * The metadata is static and doesn't change. It is sent when initializing a counter and it is kept locally in all the
 * nodes. This avoids transfer information about the counter in every operation (e.g. boundaries/reset).
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ConfigurationMetadata implements MetaParam.Writable<CounterConfiguration> {

   public static final AdvancedExternalizer<ConfigurationMetadata> EXTERNALIZER = new Externalizer();

   private final CounterConfiguration configuration;

   public ConfigurationMetadata(CounterConfiguration configuration) {
      this.configuration = configuration;
   }


   @Override
   public CounterConfiguration get() {
      return configuration;
   }

   public static class Externalizer implements AdvancedExternalizer<ConfigurationMetadata> {

      @Override
      public Set<Class<? extends ConfigurationMetadata>> getTypeClasses() {
         return Collections.singleton(ConfigurationMetadata.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.COUNTER_METADATA;
      }

      @Override
      public void writeObject(ObjectOutput output, ConfigurationMetadata object) throws IOException {
         CounterConfiguration.EXTERNALIZER.writeObject(output, object.configuration);
      }

      @Override
      public ConfigurationMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ConfigurationMetadata(CounterConfiguration.EXTERNALIZER.readObject(input));
      }
   }
}
