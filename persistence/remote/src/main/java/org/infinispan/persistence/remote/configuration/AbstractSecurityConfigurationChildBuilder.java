package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * AbstractSecurityConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class AbstractSecurityConfigurationChildBuilder extends AbstractRemoteStoreConfigurationChildBuilder {
   final SecurityConfigurationBuilder builder;

   AbstractSecurityConfigurationChildBuilder(SecurityConfigurationBuilder builder, AttributeSet attributes) {
      super(builder.getRemoteStoreBuilder(), attributes);
      this.builder = builder;
   }

   public AuthenticationConfigurationBuilder authentication() {
      return builder.authentication();
   }

   public SslConfigurationBuilder ssl() {
      return builder.ssl();
   }
}
