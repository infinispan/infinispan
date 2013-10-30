package org.infinispan.configuration.cache;

abstract class AbstractSecurityConfigurationChildBuilder extends AbstractConfigurationChildBuilder implements SecurityConfigurationChildBuilder {

   private final SecurityConfigurationBuilder securityBuilder;

   protected AbstractSecurityConfigurationChildBuilder(SecurityConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.securityBuilder = builder;
   }

   protected SecurityConfigurationBuilder getSecurityBuilder() {
      return securityBuilder;
   }

   @Override
   public AuthorizationConfigurationBuilder authorization() {
      return securityBuilder.authorization();
   }

}
