package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;

public class MockServerConfiguration extends ProtocolServerConfiguration {

   protected MockServerConfiguration(AttributeSet attributes, SslConfiguration ssl) {
      super(attributes, ssl);
   }
}
