package org.infinispan.client.hotrod.configuration;

/**
 * AbstractSecurityConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AbstractSecurityConfigurationChildBuilder extends AbstractConfigurationChildBuilder {
   private final SecurityConfigurationBuilder builder;

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
