package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 15.0
 **/
public class MockAuthenticationConfigurationBuilder implements AuthenticationConfigurationBuilder<MockAuthenticationConfiguration> {
   @Override
   public MockAuthenticationConfiguration create() {
      return new MockAuthenticationConfiguration();
   }

   @Override
   public Builder<?> read(MockAuthenticationConfiguration template, Combine combine) {
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   @Override
   public MockAuthenticationConfigurationBuilder enable() {
      return this;
   }

   @Override
   public String securityRealm() {
      return "default";
   }
}
