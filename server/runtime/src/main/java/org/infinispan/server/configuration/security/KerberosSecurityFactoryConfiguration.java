package org.infinispan.server.configuration.security;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * KerberosSecurityFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.1
 */
public class KerberosSecurityFactoryConfiguration implements ConfigurationInfo {
   private static final String[] DEFAULT_MECHANISM_NAMES = new String[]{"KRB5", "SPNEGO"};

   static final AttributeDefinition<String> PRINCIPAL = AttributeDefinition.builder("principal", null, String.class).build();
   static final AttributeDefinition<String> KEYTAB_PATH = AttributeDefinition.builder("keytabPath", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();
   static final AttributeDefinition<Boolean> DEBUG = AttributeDefinition.builder("debug", false, Boolean.class).build();
   static final AttributeDefinition<Long> FAIL_CACHE = AttributeDefinition.builder("failCache", 0l, Long.class).build();
   static final AttributeDefinition<Set<String>> MECHANISM_NAMES = AttributeDefinition.<Set<String>>builder("mechanismNames", new HashSet<>()).initializer(() -> new HashSet<>(Arrays.asList(DEFAULT_MECHANISM_NAMES))).build();
   static final AttributeDefinition<Set<String>> MECHANISM_OIDS = AttributeDefinition.<Set<String>>builder("mechanismOids", new HashSet<>()).initializer(HashSet::new).build();
   static final AttributeDefinition<Integer> MINIMUM_REMAINING_LIFETIME = AttributeDefinition.builder("minimumRemainingLifetime", 0, Integer.class).build();
   static final AttributeDefinition<Boolean> OBTAIN_KERBEROS_TICKET = AttributeDefinition.builder("obtainKerberosTicket", false, Boolean.class).build();
   static final AttributeDefinition<Map<String, Object>> OPTIONS = AttributeDefinition.<Map<String, Object>>builder("options", new HashMap<>()).initializer(HashMap::new).build();
   static final AttributeDefinition<Integer> REQUEST_LIFETIME = AttributeDefinition.builder("requiredLifetime", 0, Integer.class).build();
   static final AttributeDefinition<Boolean> REQUIRED = AttributeDefinition.builder("required", false, Boolean.class).build();
   static final AttributeDefinition<Boolean> SERVER = AttributeDefinition.builder("server", true, Boolean.class).build();
   static final AttributeDefinition<Boolean> WRAP_GSS_CREDENTIAL = AttributeDefinition.builder("wrapGssCredential", false, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KerberosSecurityFactoryConfiguration.class, PRINCIPAL, KEYTAB_PATH, RELATIVE_TO, DEBUG, FAIL_CACHE,
            MECHANISM_NAMES, MECHANISM_OIDS, MINIMUM_REMAINING_LIFETIME, OBTAIN_KERBEROS_TICKET, OPTIONS, REQUEST_LIFETIME,
            REQUIRED, SERVER, WRAP_GSS_CREDENTIAL);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.KERBEROS.toString());

   private final AttributeSet attributes;
   private final CredentialSource credentialSource;

   KerberosSecurityFactoryConfiguration(AttributeSet attributes, CredentialSource credentialSource) {
      this.attributes = attributes.checkProtection();
      this.credentialSource = credentialSource;
   }

   public String getPrincipal() {
      return attributes.attribute(PRINCIPAL).get();
   }

   public CredentialSource getCredentialSource() {
      return credentialSource;
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
