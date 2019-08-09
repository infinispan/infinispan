package org.infinispan.rest.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

import io.netty.handler.codec.http.cors.CorsConfig;

/**
 * @since 10.0
 */
public class CorsRuleConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> ALLOW_CREDENTIALS = AttributeDefinition.builder("allow-credentials", null, Boolean.class).immutable().build();
   static final AttributeDefinition<Long> MAX_AGE = AttributeDefinition.builder("max-age-seconds", null, Long.class).immutable().build();
   static final AttributeDefinition<String[]> ALLOW_ORIGINS = AttributeDefinition.builder("allowed-origins", null, String[].class).immutable().build();
   static final AttributeDefinition<String[]> ALLOW_METHODS = AttributeDefinition.builder("allowed-methods", null, String[].class).immutable().build();
   static final AttributeDefinition<String[]> ALLOW_HEADERS = AttributeDefinition.builder("allowed-headers", null, String[].class).immutable().build();
   static final AttributeDefinition<String[]> EXPOSE_HEADERS = AttributeDefinition.builder("expose-headers", null, String[].class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CorsRuleConfiguration.class, NAME, ALLOW_CREDENTIALS, MAX_AGE, ALLOW_ORIGINS, ALLOW_METHODS, ALLOW_HEADERS, EXPOSE_HEADERS);
   }

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("cors-rule");

   private final AttributeSet attributes;
   private final CorsConfig corsConfig;

   CorsRuleConfiguration(AttributeSet attributes, CorsConfig corsConfig) {
      this.attributes = attributes.checkProtection();
      this.corsConfig = corsConfig;
   }

   CorsConfig corsConfig() {
      return corsConfig;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CorsRuleConfiguration that = (CorsRuleConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      return corsConfig.equals(that.corsConfig);
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + corsConfig.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "CorsConfiguration{" +
            "attributes=" + attributes +
            ", corsConfig=" + corsConfig +
            '}';
   }
}
