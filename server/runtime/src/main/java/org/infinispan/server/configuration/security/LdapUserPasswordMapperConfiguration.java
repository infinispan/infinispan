package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class LdapUserPasswordMapperConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> FROM = AttributeDefinition.builder("from", null, String.class).build();
   static final AttributeDefinition<Boolean> VERIFIABLE = AttributeDefinition.builder("verifiable", null, Boolean.class).build();
   static final AttributeDefinition<Boolean> WRITABLE = AttributeDefinition.builder("writable", null, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapUserPasswordMapperConfiguration.class, FROM, VERIFIABLE, WRITABLE);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.USER_PASSWORD_MAPPER.toString());

   private final AttributeSet attributes;

   LdapUserPasswordMapperConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
