package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class LdapUserPasswordMapperConfiguration extends ConfigurationElement<LdapUserPasswordMapperConfiguration> {
   static final AttributeDefinition<String> FROM = AttributeDefinition.builder(Attribute.FROM, null, String.class).build();
   static final AttributeDefinition<Boolean> VERIFIABLE = AttributeDefinition.builder(Attribute.VERIFIABLE, null, Boolean.class).build();
   static final AttributeDefinition<Boolean> WRITABLE = AttributeDefinition.builder(Attribute.WRITABLE, null, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapUserPasswordMapperConfiguration.class, FROM, VERIFIABLE, WRITABLE);
   }
   LdapUserPasswordMapperConfiguration(AttributeSet attributes) {
      super(Element.USER_PASSWORD_MAPPER, attributes);
   }
}
