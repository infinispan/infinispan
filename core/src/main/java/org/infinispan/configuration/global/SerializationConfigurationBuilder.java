package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.SerializationConfiguration.ADVANCED_EXTERNALIZERS;
import static org.infinispan.configuration.global.SerializationConfiguration.CLASS_RESOLVER;
import static org.infinispan.configuration.global.SerializationConfiguration.MARSHALLER;
import static org.infinispan.configuration.global.SerializationConfiguration.SERIALIZATION_CONTEXT_INITIALIZERS;
import static org.infinispan.configuration.global.SerializationConfiguration.VERSION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Version;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Configures serialization and marshalling settings.
 */
public class SerializationConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<SerializationConfiguration> {
   private final AttributeSet attributes;
   private final WhiteListConfigurationBuilder whiteListBuilder;
   private Map<Integer, AdvancedExternalizer<?>> advancedExternalizers = new HashMap<>();

   SerializationConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.whiteListBuilder = new WhiteListConfigurationBuilder();
      this.attributes = SerializationConfiguration.attributeDefinitionSet();
   }

   /**
    * Set the marshaller instance that will marshall and unmarshall cache entries.
    *
    * @param marshaller
    */
   public SerializationConfigurationBuilder marshaller(Marshaller marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   public Marshaller getMarshaller() {
      return attributes.attribute(MARSHALLER).get();
   }


   /**
    * Largest allowable version to use when marshalling internal state. Set this to the lowest version cache instance in
    * your cluster to ensure compatibility of communications. However, setting this too low will mean you lose out on
    * the benefit of improvements in newer versions of the marshaller.
    *
    * @param marshallVersion
    */
   public SerializationConfigurationBuilder version(short marshallVersion) {
      attributes.attribute(VERSION).set(marshallVersion);
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
      this.version(Version.getVersionShort(marshallVersion));
      return this;
   }

   /**
    * Helper method that allows for quick registration of an {@link AdvancedExternalizer}
    * implementation alongside its corresponding identifier. Remember that the identifier needs to a be positive number,
    * including 0, and cannot clash with other identifiers in the system.
    *
    * @param id
    * @param advancedExternalizer
    * @deprecated since 10.0, {@link AdvancedExternalizer}'s will be removed in a future release. Please utilise
    * ProtoStream annotations on Java objects instead by configuring a {@link org.infinispan.protostream.SerializationContextInitializer}
    * via {@link #addContextInitializer(SerializationContextInitializer)}, or specifying a custom marshaller for user types
    * via {@link #marshaller(Marshaller)}.
    */
   @Deprecated
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(int id, AdvancedExternalizer<T> advancedExternalizer) {
      AdvancedExternalizer<?> ext = advancedExternalizers.get(id);
      if (ext != null)
         throw new CacheConfigurationException(String.format(
               "Duplicate externalizer id found! Externalizer id=%d for %s is shared by another externalizer (%s)",
               id, advancedExternalizer.getClass().getName(), ext.getClass().getName()));

      advancedExternalizers.put(id, advancedExternalizer);
      return this;
   }

   /**
    * Helper method that allows for quick registration of an {@link AdvancedExternalizer}
    * implementation alongside its corresponding identifier. Remember that the identifier needs to a be positive number,
    * including 0, and cannot clash with other identifiers in the system.
    *
    * @param advancedExternalizer
    * @deprecated since 10.0, {@link AdvancedExternalizer}'s will be removed in a future release. Please utilise
    * ProtoStream annotations on Java objects instead by configuring a {@link org.infinispan.protostream.SerializationContextInitializer}
    * via {@link #addContextInitializer(SerializationContextInitializer)}, or specifying a custom marshaller for user types
    * via {@link #marshaller(Marshaller)}.
    */
   @Deprecated
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(AdvancedExternalizer<T> advancedExternalizer) {
      Integer id = advancedExternalizer.getId();
      if (id == null)
         throw new CacheConfigurationException(String.format(
               "No advanced externalizer identifier set for externalizer %s",
               advancedExternalizer.getClass().getName()));

      this.addAdvancedExternalizer(id.intValue(), advancedExternalizer);
      return this;
   }

   /**
    * Helper method that allows for quick registration of {@link AdvancedExternalizer} implementations.
    *
    * @param advancedExternalizers
    * @deprecated since 10.0, {@link AdvancedExternalizer}'s will be removed in a future release. Please utilise
    * ProtoStream annotations on Java objects instead by configuring a {@link org.infinispan.protostream.SerializationContextInitializer}
    * via {@link #addContextInitializer(SerializationContextInitializer)}, or specifying a custom marshaller for user types
    * via {@link #marshaller(Marshaller)}.
    */
   @Deprecated
   public <T> SerializationConfigurationBuilder addAdvancedExternalizer(AdvancedExternalizer<T>... advancedExternalizers) {
      for (AdvancedExternalizer<T> advancedExternalizer : advancedExternalizers) {
         this.addAdvancedExternalizer(advancedExternalizer);
      }
      return this;
   }

   /**
    * Class resolver to use when unmarshalling objects.
    *
    * @param classResolver
    * @deprecated since 10.0 {@link org.jboss.marshalling.ClassResolver} is specific to jboss-marshalling and will be removed in a future version.
    */
   @Deprecated
   public SerializationConfigurationBuilder classResolver(Object classResolver) {
      attributes.attribute(CLASS_RESOLVER).set(classResolver);
      return this;
   }

   public SerializationConfigurationBuilder addContextInitializer(SerializationContextInitializer sci) {
      if (sci == null)
         throw new CacheConfigurationException("SerializationContextInitializer cannot be null");
      attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS).computeIfAbsent(ArrayList::new).add(sci);
      return this;
   }

   public SerializationConfigurationBuilder addContextInitializers(SerializationContextInitializer... scis) {
      return addContextInitializers(Arrays.asList(scis));
   }

   public SerializationConfigurationBuilder addContextInitializers(List<SerializationContextInitializer> scis) {
      attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS).computeIfAbsent(ArrayList::new).addAll(scis);
      return this;
   }

   public WhiteListConfigurationBuilder whiteList() {
      return whiteListBuilder;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public
   SerializationConfiguration create() {
      if (!advancedExternalizers.isEmpty()) attributes.attribute(ADVANCED_EXTERNALIZERS).set(advancedExternalizers);
      return new SerializationConfiguration(attributes.protect(), whiteListBuilder.create());
   }

   @Override
   public
   SerializationConfigurationBuilder read(SerializationConfiguration template) {
      this.attributes.read(template.attributes());
      this.advancedExternalizers = template.advancedExternalizers();
      this.whiteListBuilder.read(template.whiteList());
      return this;
   }

   @Override
   public String toString() {
      return "SerializationConfigurationBuilder [attributes=" + attributes + ", advancedExternalizers=" + advancedExternalizers + "]";
   }

}
