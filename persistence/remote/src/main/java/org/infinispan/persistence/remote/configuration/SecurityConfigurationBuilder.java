package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

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
   public Builder<?> read(SecurityConfiguration template, Combine combine) {
      authentication.read(template.authentication(), combine);
      ssl.read(template.ssl(), combine);
      return this;
   }

   public AttributeSet attributes() {
      return attributes;
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
