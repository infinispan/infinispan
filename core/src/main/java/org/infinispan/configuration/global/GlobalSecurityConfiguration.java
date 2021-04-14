package org.infinispan.configuration.global;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * GlobalSecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalSecurityConfiguration {
   private final GlobalAuthorizationConfiguration authorization;
   public static final AttributeDefinition<Integer> CACHE_SIZE = AttributeDefinition.builder("securityCacheSize", 1000).build();
   public static final AttributeDefinition<Long> CACHE_TIMEOUT = AttributeDefinition.builder("securityCacheTimeout", TimeUnit.MINUTES.toMillis(5)).build();


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalSecurityConfiguration.class, CACHE_SIZE, CACHE_TIMEOUT);
   }

   private final AttributeSet attributes;

   public GlobalSecurityConfiguration(GlobalAuthorizationConfiguration authorization, AttributeSet attributes) {
      this.authorization = authorization;
      this.attributes = attributes;
   }

   public GlobalAuthorizationConfiguration authorization() {
      return authorization;
   }

   public long securityCacheSize() {
      return attributes.attribute(CACHE_SIZE).get();
   }

   public long securityCacheTimeout() {
      return attributes.attribute(CACHE_TIMEOUT).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GlobalSecurityConfiguration that = (GlobalSecurityConfiguration) o;
      return Objects.equals(authorization, that.authorization) && Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return Objects.hash(authorization, attributes);
   }

   @Override
   public String toString() {
      return "GlobalSecurityConfiguration{" +
            "authorization=" + authorization +
            ", attributes=" + attributes +
            '}';
   }
}
