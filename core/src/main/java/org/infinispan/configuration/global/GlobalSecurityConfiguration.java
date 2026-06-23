package org.infinispan.configuration.global;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * GlobalSecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@BuiltBy(GlobalSecurityConfigurationBuilder.class)
public class GlobalSecurityConfiguration extends ConfigurationElement<GlobalSecurityConfiguration> {
   private final GlobalAuthorizationConfiguration authorization;
   public static final AttributeDefinition<Integer> CACHE_SIZE = AttributeDefinition.builder("securityCacheSize", 1000).build();
   public static final AttributeDefinition<Long> CACHE_TIMEOUT = AttributeDefinition.builder("securityCacheTimeout", TimeUnit.MINUTES.toMillis(5)).build();


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalSecurityConfiguration.class, CACHE_SIZE, CACHE_TIMEOUT);
   }

   public GlobalSecurityConfiguration(GlobalAuthorizationConfiguration authorization, AttributeSet attributes) {
      super(Element.SECURITY, attributes, authorization);
      this.authorization = authorization;
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
}
