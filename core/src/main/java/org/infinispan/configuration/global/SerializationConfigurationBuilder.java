package org.infinispan.configuration.global;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;

/**
 * Configures serialization and marshalling settings.
 */
public class SerializationConfigurationBuilder extends AbstractGlobalConfigurationBuilder<SerializationConfiguration> {
   
   private Class<? extends Marshaller> marshallerClass = VersionAwareMarshaller.class;
   private short marshallVersion;
   private Map<Integer, AdvancedExternalizer<?>> advancedExternalizers = new HashMap<Integer, AdvancedExternalizer<?>>();
   
   SerializationConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }
   
   /**
    * Fully qualified name of the marshaller to use. It must implement
    * org.infinispan.marshall.StreamingMarshaller
    *
    * @param marshallerClass
    */
   public SerializationConfigurationBuilder marshallerClass(Class<? extends Marshaller> marshallerClass) {
      this.marshallerClass = marshallerClass;
      return this;
   }

   /**
    * Largest allowable version to use when marshalling internal state. Set this to the lowest
    * version cache instance in your cluster to ensure compatibility of communications. However,
    * setting this too low will mean you lose out on the benefit of improvements in newer
    * versions of the marshaller.
    *
    * @param marshallVersion
    */
   public SerializationConfigurationBuilder version(short marshallVersion) {
      this.marshallVersion = marshallVersion;
      return this;
   }

   /**
    * Helper method that allows for quick registration of an {@link org.infinispan.marshall.AdvancedExternalizer} implementation
    * alongside its corresponding identifier. Remember that the identifier needs to a be positive
    * number, including 0, and cannot clash with other identifiers in the system.
    *
    * @param id
    * @param advancedExternalizer
    */
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(int id, AdvancedExternalizer<T> advancedExternalizer) {
      advancedExternalizers.put(id, advancedExternalizer);
      return this;
   }
   
   /**
    * Helper method that allows for quick registration of an {@link org.infinispan.marshall.AdvancedExternalizer} implementation
    * alongside its corresponding identifier. Remember that the identifier needs to a be positive
    * number, including 0, and cannot clash with other identifiers in the system.
    *
    * @param id
    * @param advancedExternalizer
    */
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(AdvancedExternalizer<T> advancedExternalizer) {
      this.addAdvancedExternalizer(advancedExternalizer.getId(), advancedExternalizer);
      return this;
   }

   /**
    * Helper method that allows for quick registration of {@link org.infinispan.marshall.AdvancedExternalizer} implementations.
    *
    * @param advancedExternalizers
    */
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(AdvancedExternalizer<T>... advancedExternalizers) {
      for (AdvancedExternalizer<T> advancedExternalizer : advancedExternalizers)  {
         this.addAdvancedExternalizer(advancedExternalizer);
      }
      return this;
   }
   
   @Override
   protected void valididate() {
      // No-op, no validation required
   }
   
   @Override
   SerializationConfiguration create() {
      return new SerializationConfiguration(marshallerClass, marshallVersion, advancedExternalizers);
   }
}