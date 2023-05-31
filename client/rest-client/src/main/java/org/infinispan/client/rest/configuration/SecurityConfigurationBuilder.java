package org.infinispan.client.rest.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

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

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
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
   public Builder<?> read(SecurityConfiguration template, Combine combine) {
      authentication.read(template.authentication(), combine);
      ssl.read(template.ssl(), combine);
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
