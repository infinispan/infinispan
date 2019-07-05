package org.infinispan.client.rest.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * SecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class SecurityConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      Builder<SecurityConfiguration> {

   private final AuthenticationConfigurationBuilder authentication = new AuthenticationConfigurationBuilder(this);
   private final SslConfigurationBuilder ssl = new SslConfigurationBuilder(this);

   SecurityConfigurationBuilder(RestClientConfigurationBuilder builder) {
      super(builder);
   }

   public AuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public SslConfigurationBuilder ssl() {
      return ssl;
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(authentication.create(), ssl.create());
   }

   @Override
   public Builder<?> read(SecurityConfiguration template) {
      authentication.read(template.authentication());
      ssl.read(template.ssl());
      return this;
   }

   @Override
   public void validate() {
      authentication.validate();
      ssl.validate();
   }

   RestClientConfigurationBuilder getBuilder() {
      return super.builder;
   }

}
