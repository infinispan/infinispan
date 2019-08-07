package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.realm.KerberosSecurityRealm;

/**
 * @since 10.0
 */
public class KerberosRealmConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> KEYTAB_PATH = AttributeDefinition.builder("keytabPath", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KerberosRealmConfiguration.class, KEYTAB_PATH, RELATIVE_TO);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.KERBEROS_REALM.toString());

   private final AttributeSet attributes;
   private final KerberosSecurityRealm securityRealm;

   KerberosRealmConfiguration(AttributeSet attributes, KerberosSecurityRealm securityRealm) {
      this.attributes = attributes.checkProtection();
      this.securityRealm = securityRealm;
   }

   public KerberosSecurityRealm getSecurityRealm() {
      return securityRealm;
   }

   public String keyTabPath() {
      return attributes.attribute(KEYTAB_PATH).get();
   }

   public String relativeTo() {
      return attributes.attribute(RELATIVE_TO).get();
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
