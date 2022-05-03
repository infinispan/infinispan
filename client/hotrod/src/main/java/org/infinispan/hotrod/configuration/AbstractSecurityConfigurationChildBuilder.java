package org.infinispan.hotrod.configuration;

/**
 * AbstractSecurityConfigurationChildBuilder.
 *
 * @since 14.0
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
