package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 15.0
 **/
public class NoAuthenticationConfigurationBuilder implements AuthenticationConfigurationBuilder<NoAuthenticationConfiguration> {
   @Override
   public NoAuthenticationConfiguration create() {
      return new NoAuthenticationConfiguration();
   }

   @Override
   public Builder<?> read(NoAuthenticationConfiguration template, Combine combine) {
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   @Override
   public AuthenticationConfigurationBuilder<NoAuthenticationConfiguration> enable() {
      return this;
   }

   @Override
   public String securityRealm() {
      return null;
   }
}
