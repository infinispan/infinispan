package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;

public class MockServerConfiguration extends ProtocolServerConfiguration<MockServerConfiguration, MockAuthenticationConfiguration> {

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MockServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet());
   }

   protected MockServerConfiguration(AttributeSet attributes, MockAuthenticationConfiguration authentication, SslConfiguration ssl, IpFilterConfiguration ipRules) {
      super("mock-connector", attributes, authentication, ssl, ipRules);
   }
}
