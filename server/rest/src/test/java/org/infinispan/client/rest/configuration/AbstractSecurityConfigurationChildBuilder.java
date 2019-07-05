package org.infinispan.client.rest.configuration;

/**
 * AbstractSecurityConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
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
