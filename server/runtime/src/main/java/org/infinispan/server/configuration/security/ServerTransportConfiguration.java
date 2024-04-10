package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.server.configuration.Attribute;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class ServerTransportConfiguration extends ConfigurationElement<ServerTransportConfiguration> {
   static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder(Attribute.SECURITY_REALM, null, String.class).immutable().build();
   static final AttributeDefinition<String> DATA_SOURCE = AttributeDefinition.builder(Attribute.DATA_SOURCE, null, String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TrustStoreConfiguration.class, SECURITY_REALM, DATA_SOURCE);
   }

   ServerTransportConfiguration(AttributeSet attributes) {
      super(Element.TRANSPORT, attributes);
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }

   public String dataSource() {
      return attributes.attribute(DATA_SOURCE).get();
   }
}
