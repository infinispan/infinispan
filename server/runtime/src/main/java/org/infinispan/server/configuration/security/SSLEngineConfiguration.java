package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.ssl.CipherSuiteSelector;
import org.wildfly.security.ssl.ProtocolSelector;
import org.wildfly.security.ssl.SSLContextBuilder;

/**
 * @since 10.0
 */
public class SSLEngineConfiguration extends ConfigurationElement<SSLEngineConfiguration> {
   static final AttributeDefinition<String[]> ENABLED_PROTOCOLS = AttributeDefinition.builder(Attribute.ENABLED_PROTOCOLS, null, String[].class)
         .serializer(AttributeSerializer.STRING_ARRAY).immutable().build();
   static final AttributeDefinition<String> ENABLED_CIPHERSUITES = AttributeDefinition.builder(Attribute.ENABLED_CIPHERSUITES, null, String.class)
         .immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SSLEngineConfiguration.class, ENABLED_PROTOCOLS, ENABLED_CIPHERSUITES);
   }

   SSLEngineConfiguration(AttributeSet attributes) {
      super(Element.ENGINE, attributes);
   }

   void build(SSLContextBuilder builder) {
      attributes.attribute(ENABLED_PROTOCOLS).apply(protocols -> {
         ProtocolSelector protocolSelector = ProtocolSelector.empty();
         for (String protocol : protocols) {
            protocolSelector = protocolSelector.add(protocol);
         }
         builder.setProtocolSelector(protocolSelector);
      });
      attributes.attribute(ENABLED_CIPHERSUITES).apply(cipherSuites -> builder.setCipherSuiteSelector(CipherSuiteSelector.fromString(cipherSuites)));
   }
}
