package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;

public class MockServerConfiguration extends ProtocolServerConfiguration {

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MockServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet(), WORKER_THREADS);
   }

   protected MockServerConfiguration(AttributeSet attributes, SslConfiguration ssl, IpFilterConfiguration ipRules) {
      super("mock-connector", attributes, ssl, ipRules);
   }
}
