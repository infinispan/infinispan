package org.infinispan.server.hotrod.configuration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 10.0
 */
@SuppressWarnings("unchecked")
public class SaslConfiguration extends ConfigurationElement<SaslConfiguration> {
   public static final AttributeDefinition<String> SERVER_NAME = AttributeDefinition.builder(Attribute.SERVER_NAME, "infinispan", String.class).immutable().build();
   public static final AttributeDefinition<Set<String>> MECHANISMS = AttributeDefinition.builder(Attribute.MECHANISMS, null, (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(LinkedHashSet::new).serializer(AttributeSerializer.STRING_COLLECTION).immutable().build();
   public static final AttributeDefinition<List<QOP>> QOP = AttributeDefinition.builder(Attribute.QOP, new ArrayList<>(), (Class<List<QOP>>) (Class<?>) List.class)
         .initializer(ArrayList::new).serializer(AttributeSerializer.ENUM_COLLECTION).immutable().build();
   public static final AttributeDefinition<List<Strength>> STRENGTH = AttributeDefinition.builder(Attribute.STRENGTH, new ArrayList<>(), (Class<List<Strength>>) (Class<?>) Strength.class)
         .initializer(ArrayList::new).serializer(AttributeSerializer.ENUM_COLLECTION).immutable().build();
   public static final AttributeDefinition<List<Policy>> POLICY = AttributeDefinition.builder(Attribute.POLICY, new ArrayList<>(), (Class<List<Policy>>) (Class<?>) Policy.class)
         .initializer(ArrayList::new).serializer(AttributeSerializer.ENUM_COLLECTION).immutable().build();
   static final AttributeDefinition<Map<String, String>> SASL_PROPERTIES = AttributeDefinition.builder(Element.PROPERTY, null, (Class<Map<String, String>>) (Class<?>) Map.class).initializer(LinkedHashMap::new).autoPersist(false).immutable().build();

   private final Map<String, String> mechProperties;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SaslConfiguration.class, SERVER_NAME, MECHANISMS, QOP, STRENGTH, POLICY, SASL_PROPERTIES);
   }

   SaslConfiguration(AttributeSet attributes, Map<String, String> mechProperties) {
      super(Element.SASL, attributes);
      this.mechProperties = mechProperties;
   }

   Map<String, String> mechProperties() {
      return mechProperties;
   }

   public String serverName() {
      return attributes.attribute(SERVER_NAME).get();
   }

   public Set<String> mechanisms() {
      return attributes.attribute(MECHANISMS).get();
   }

   public List<QOP> qop() {
      return attributes.attribute(QOP).get();
   }

   public List<Strength> strength() {
      return attributes.attribute(STRENGTH).get();
   }
}
