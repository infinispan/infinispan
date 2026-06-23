package org.infinispan.configuration.global;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;

public class SerializationConfiguration extends ConfigurationElement<SerializationConfiguration> {
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MARSHALLER, null, Marshaller.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME)
         .immutable().build();
   public static final AttributeDefinition<List<SerializationContextInitializer>> SERIALIZATION_CONTEXT_INITIALIZERS =
         AttributeDefinition.builder("contextInitializers", null, (Class<List<SerializationContextInitializer>>) (Class<?>) List.class)
               .immutable().serializer(new AttributeSerializer<>() {
                  @Override
                  public void serialize(ConfigurationWriter writer, String name, List<SerializationContextInitializer> value) {
                     List<String> classes = value.stream().map(s -> s.getClass().getName()).collect(Collectors.toList());
                     writer.writeArrayElement(Element.SERIALIZATION_CONTEXT_INITIALIZERS, Element.SERIALIZATION_CONTEXT_INITIALIZER, Attribute.CLASS, classes);
                  }

                  @Override
                  public boolean defer() {
                     return true;
                  }
               }).build();
   public static final AttributeDefinition<Configuration.SchemaValidation> SCHEMA_COMPATIBILITY =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SCHEMA_COMPATIBILITY, Configuration.SchemaValidation.STRICT, Configuration.SchemaValidation.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, SERIALIZATION_CONTEXT_INITIALIZERS, SCHEMA_COMPATIBILITY);
   }

   private final AllowListConfiguration allowListConfig;

   SerializationConfiguration(AttributeSet attributes, AllowListConfiguration allowListConfig) {
      super(Element.SERIALIZATION, attributes, allowListConfig);
      this.allowListConfig = allowListConfig;
   }

   public Marshaller marshaller() {
      return attributes.attribute(MARSHALLER).get();
   }

   public List<SerializationContextInitializer> contextInitializers() {
      return attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS).get();
   }

   public Configuration.SchemaValidation schemaCompatibilityValidation() {
      return attributes.attribute(SCHEMA_COMPATIBILITY).get();
   }

   public AllowListConfiguration allowList() {
      return allowListConfig;
   }
}
