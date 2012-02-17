package org.infinispan.configuration.global;

import org.infinispan.Version;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import java.util.HashMap;
import java.util.Map;

/**
 * Configures serialization and marshalling settings.
 */
public class SerializationConfigurationBuilder extends AbstractGlobalConfigurationBuilder<SerializationConfiguration> {

   private Marshaller marshaller = new VersionAwareMarshaller();
   private short marshallVersion = Short.valueOf(Version.MAJOR_MINOR.replace(".", ""));
   private Map<Integer, AdvancedExternalizer<?>> advancedExternalizers = new HashMap<Integer, AdvancedExternalizer<?>>();

   SerializationConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   /**
    * Set the marshaller instance that will marshall and unmarshall cache entries.
    *
    * @param marshaller
    */
   public SerializationConfigurationBuilder marshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
      return this;
   }


   /**
    * Largest allowable version to use when marshalling internal state. Set this to the lowest version cache instance in
    * your cluster to ensure compatibility of communications. However, setting this too low will mean you lose out on
    * the benefit of improvements in newer versions of the marshaller.
    *
    * @param marshallVersion
    */
   public SerializationConfigurationBuilder version(short marshallVersion) {
      this.marshallVersion = marshallVersion;
      return this;
   }

   /**
    * Largest allowable version to use when marshalling internal state. Set this to the lowest version cache instance in
    * your cluster to ensure compatibility of communications. However, setting this too low will mean you lose out on
    * the benefit of improvements in newer versions of the marshaller.
    *
    * @param marshallVersion
    */
   public SerializationConfigurationBuilder version(String marshallVersion) {
      this.marshallVersion = Version.getVersionShort(marshallVersion);
      return this;
   }

   /**
    * Helper method that allows for quick registration of an {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementation alongside its corresponding identifier. Remember that the identifier needs to a be positive number,
    * including 0, and cannot clash with other identifiers in the system.
    *
    * @param id
    * @param advancedExternalizer
    */
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(int id, AdvancedExternalizer<T> advancedExternalizer) {
      advancedExternalizers.put(id, advancedExternalizer);
      return this;
   }

   /**
    * Helper method that allows for quick registration of an {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementation alongside its corresponding identifier. Remember that the identifier needs to a be positive number,
    * including 0, and cannot clash with other identifiers in the system.
    *
    * @param advancedExternalizer
    */
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(AdvancedExternalizer<T> advancedExternalizer) {
      this.addAdvancedExternalizer(advancedExternalizer.getId(), advancedExternalizer);
      return this;
   }

   /**
    * Helper method that allows for quick registration of {@link org.infinispan.marshall.AdvancedExternalizer}
    * implementations.
    *
    * @param advancedExternalizers
    */
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(AdvancedExternalizer<T>... advancedExternalizers) {
      for (AdvancedExternalizer<T> advancedExternalizer : advancedExternalizers) {
         this.addAdvancedExternalizer(advancedExternalizer);
      }
      return this;
   }

   @Override
   protected void validate() {
      // No-op, no validation required
   }

   @Override
   SerializationConfiguration create() {
      return new SerializationConfiguration(marshaller, marshallVersion, advancedExternalizers);
   }

   @Override
   SerializationConfigurationBuilder read(SerializationConfiguration template) {
      this.advancedExternalizers = template.advancedExternalizers();
      this.marshaller = template.marshaller();
      this.marshallVersion = template.version();

      return this;
   }

   @Override
   public String toString() {
      return "SerializationConfigurationBuilder{" +
            "advancedExternalizers=" + advancedExternalizers +
            ", marshaller=" + marshaller +
            ", marshallVersion=" + marshallVersion +
            '}';
   }

}