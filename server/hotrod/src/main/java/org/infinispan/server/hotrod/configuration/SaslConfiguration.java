package org.infinispan.server.hotrod.configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
@SuppressWarnings("unchecked")
public class SaslConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> SERVER_NAME = AttributeDefinition.builder("serverName", null, String.class).immutable().build();
   public static final AttributeDefinition<Set<String>> MECHANISMS = AttributeDefinition.builder("mechanisms", null, (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(LinkedHashSet::new).immutable().build();
   public static final AttributeDefinition<List<QOP>> QOP = AttributeDefinition.builder("qop", new ArrayList<>(), (Class<List<QOP>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().build();
   public static final AttributeDefinition<List<Strength>> STRENGTH = AttributeDefinition.builder("strength", new ArrayList<>(), (Class<List<Strength>>) (Class<?>) Strength.class).initializer(ArrayList::new).immutable().build();
   static final AttributeDefinition<Map<String, String>> SASL_PROPERTIES = AttributeDefinition.builder("property", null, (Class<Map<String, String>>) (Class<?>) Map.class).initializer(LinkedHashMap::new).autoPersist(false).immutable().build();

   private final AttributeSet attributes;
   private final Map<String, String> mechProperties;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SaslConfiguration.class, SERVER_NAME, MECHANISMS, QOP, STRENGTH, SASL_PROPERTIES);
   }

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("sasl");

   private final PolicyConfiguration policy;

   SaslConfiguration(AttributeSet attributes, Map<String, String> mechProperties, PolicyConfiguration policy) {
      this.attributes = attributes.checkProtection();
      this.mechProperties = mechProperties;
      this.policy = policy;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return Collections.singletonList(policy);
   }

   Map<String, String> mechProperties() {
      return mechProperties;
   }

   public PolicyConfiguration policy() {
      return policy;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   @Override
   public String toString() {
      return "SaslConfiguration{" +
            "attributes=" + attributes +
            ", mechProperties=" + mechProperties +
            ", policy=" + policy +
            '}';
   }
}
