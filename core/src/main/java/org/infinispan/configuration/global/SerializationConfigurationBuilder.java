package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.SerializationConfiguration.MARSHALLER;
import static org.infinispan.configuration.global.SerializationConfiguration.SCHEMA_COMPATIBILITY;
import static org.infinispan.configuration.global.SerializationConfiguration.SERIALIZATION_CONTEXT_INITIALIZERS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;

/**
 * Configures serialization and marshalling settings.
 */
public class SerializationConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<SerializationConfiguration> {
   private final AttributeSet attributes;
   private final AllowListConfigurationBuilder allowListBuilder;
   private final Map<String, NamedMarshallerConfigurationBuilder> namedMarshallerBuilders = new LinkedHashMap<>();

   SerializationConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.allowListBuilder = new AllowListConfigurationBuilder(globalConfig);
      this.attributes = SerializationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Set the marshaller instance that will marshall and unmarshall cache entries.
    *
    * @param marshaller the marshaller to use
    */
   public SerializationConfigurationBuilder marshaller(Marshaller marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   public Marshaller getMarshaller() {
      return attributes.attribute(MARSHALLER).get();
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

   public AllowListConfigurationBuilder allowList() {
      return allowListBuilder;
   }

   public SerializationConfigurationBuilder schemaCompatibilityValidation(Configuration.SchemaValidation schemaValidation) {
      attributes.attribute(SCHEMA_COMPATIBILITY).set(schemaValidation);
      return this;
   }

   /**
    * Adds a named marshaller by class name.
    *
    * @param name the name for this marshaller
    * @param marshallerClassName the fully qualified class name of the marshaller
    * @return this builder for method chaining
    * @throws IllegalArgumentException if a marshaller with the given name already exists
    */
   public SerializationConfigurationBuilder addNamedMarshaller(String name, String marshallerClassName) {
      if (namedMarshallerBuilders.containsKey(name)) {
         throw new IllegalArgumentException("A marshaller with name '" + name + "' is already registered");
      }
      NamedMarshallerConfigurationBuilder builder = new NamedMarshallerConfigurationBuilder(getGlobalConfig());
      builder.name(name).marshallerClass(marshallerClassName);
      namedMarshallerBuilders.put(name, builder);
      return this;
   }

   /**
    * Adds a named marshaller by instance.
    *
    * @param name the name for this marshaller
    * @param marshaller the marshaller instance
    * @return this builder for method chaining
    * @throws IllegalArgumentException if a marshaller with the given name already exists
    */
   public SerializationConfigurationBuilder addNamedMarshaller(String name, Marshaller marshaller) {
      if (namedMarshallerBuilders.containsKey(name)) {
         throw new IllegalArgumentException("A marshaller with name '" + name + "' is already registered");
      }
      NamedMarshallerConfigurationBuilder builder = new NamedMarshallerConfigurationBuilder(getGlobalConfig());
      builder.name(name).marshaller(marshaller);
      namedMarshallerBuilders.put(name, builder);
      return this;
   }

   @Override
   public void validate() {
      // No validation needed - all builders are added with both name and marshaller
   }

   @Override
   public
   SerializationConfiguration create() {
      List<NamedMarshallerConfiguration> namedMarshallers = new ArrayList<>(namedMarshallerBuilders.size());
      for (NamedMarshallerConfigurationBuilder builder : namedMarshallerBuilders.values()) {
         namedMarshallers.add(builder.create());
      }
      return new SerializationConfiguration(attributes.protect(), allowListBuilder.create(), namedMarshallers);
   }

   @Override
   public
   SerializationConfigurationBuilder read(SerializationConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.allowListBuilder.read(template.allowList(), combine);

      namedMarshallerBuilders.clear();
      for (NamedMarshallerConfiguration config : template.namedMarshallers()) {
         NamedMarshallerConfigurationBuilder builder = new NamedMarshallerConfigurationBuilder(getGlobalConfig());
         builder.read(config, combine);
         namedMarshallerBuilders.put(config.name(), builder);
      }

      return this;
   }

   @Override
   public String toString() {
      return "SerializationConfigurationBuilder [attributes=" + attributes + "]";
   }
}
