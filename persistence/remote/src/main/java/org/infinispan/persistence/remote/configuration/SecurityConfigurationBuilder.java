package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * SecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SecurityConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder implements
      Builder<SecurityConfiguration> {

   private final AuthenticationConfigurationBuilder authentication = new AuthenticationConfigurationBuilder(this);
   private final SslConfigurationBuilder ssl = new SslConfigurationBuilder(this);

   SecurityConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder, null);
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

   @Override
   public String toString() {
      return "SecurityConfigurationBuilder{" +
            "authentication=" + authentication +
            ", ssl=" + ssl +
            '}';
   }
}
