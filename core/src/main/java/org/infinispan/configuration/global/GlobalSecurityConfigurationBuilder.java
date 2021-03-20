package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalSecurityConfiguration.CACHE_SIZE;
import static org.infinispan.configuration.global.GlobalSecurityConfiguration.CACHE_TIMEOUT;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * GlobalSecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalSecurityConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements GlobalSecurityConfigurationChildBuilder, Builder<GlobalSecurityConfiguration> {
   private final GlobalAuthorizationConfigurationBuilder authorizationBuilder;
   private final AttributeSet attributes;

   public GlobalSecurityConfigurationBuilder(GlobalConfigurationBuilder builder) {
      super(builder);
      this.authorizationBuilder = new GlobalAuthorizationConfigurationBuilder(this);
      this.attributes = GlobalSecurityConfiguration.attributeDefinitionSet();
   }

   @Override
   public GlobalAuthorizationConfigurationBuilder authorization() {
      return authorizationBuilder;
   }

   @Override
   public GlobalSecurityConfigurationBuilder securityCacheSize(int securityCacheSize) {
      attributes.attribute(CACHE_SIZE).set(securityCacheSize);
      return this;
   }

   @Override
   public GlobalSecurityConfigurationBuilder securityCacheTimeout(long securityCacheTimeout, TimeUnit unit) {
      attributes.attribute(CACHE_TIMEOUT).set(unit.toMillis(securityCacheTimeout));
      return this;
   }

   @Override
   public void validate() {
      authorizationBuilder.validate();
   }

   @Override
   public GlobalSecurityConfiguration create() {
      return new GlobalSecurityConfiguration(authorizationBuilder.create(), attributes.protect());
   }

   @Override
   public GlobalSecurityConfigurationBuilder read(GlobalSecurityConfiguration template) {
      this.authorizationBuilder.read(template.authorization());
      this.attributes.read(template.attributes());
      return this;
   }
}
