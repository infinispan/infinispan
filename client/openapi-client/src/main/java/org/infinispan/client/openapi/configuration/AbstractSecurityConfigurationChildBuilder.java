package org.infinispan.client.openapi.configuration;

/**
 * AbstractSecurityConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public class AbstractSecurityConfigurationChildBuilder extends AbstractConfigurationChildBuilder {
   final SecurityConfigurationBuilder builder;

   AbstractSecurityConfigurationChildBuilder(SecurityConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.builder = builder;
   }

   public AuthenticationConfigurationBuilder authentication() {
      return builder.authentication();
   }

   public SslConfigurationBuilder ssl() {
      return builder.ssl();
   }
}
