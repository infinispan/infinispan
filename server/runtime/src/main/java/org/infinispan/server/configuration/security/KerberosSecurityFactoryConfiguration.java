package org.infinispan.server.configuration.security;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * KerberosSecurityFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.1
 */
public class KerberosSecurityFactoryConfiguration extends ConfigurationElement<KerberosSecurityFactoryConfiguration> {
   private static final String[] DEFAULT_MECHANISM_NAMES = new String[]{"KRB5", "SPNEGO"};

   static final AttributeDefinition<String> PRINCIPAL = AttributeDefinition.builder(Attribute.PRINCIPAL, null, String.class).build();
   static final AttributeDefinition<String> KEYTAB_PATH = AttributeDefinition.builder(Attribute.KEYTAB_PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).build();
   static final AttributeDefinition<Boolean> DEBUG = AttributeDefinition.builder(Attribute.DEBUG, false, Boolean.class).build();
   static final AttributeDefinition<Long> FAIL_CACHE = AttributeDefinition.builder(Attribute.FAIL_CACHE, 0l, Long.class).build();
   static final AttributeDefinition<Set<String>> MECHANISM_NAMES = AttributeDefinition.<Set<String>>builder(Attribute.MECHANISM_NAMES, new HashSet<>()).initializer(() -> new HashSet<>(Arrays.asList(DEFAULT_MECHANISM_NAMES))).build();
   static final AttributeDefinition<Set<String>> MECHANISM_OIDS = AttributeDefinition.<Set<String>>builder(Attribute.MECHANISM_OIDS, new HashSet<>()).initializer(HashSet::new).build();
   static final AttributeDefinition<Integer> MINIMUM_REMAINING_LIFETIME = AttributeDefinition.builder(Attribute.MINIMUM_REMAINING_LIFETIME, 0, Integer.class).build();
   static final AttributeDefinition<Boolean> OBTAIN_KERBEROS_TICKET = AttributeDefinition.builder(Attribute.OBTAIN_KERBEROS_TICKET, false, Boolean.class).build();
   static final AttributeDefinition<Map<String, Object>> OPTIONS = AttributeDefinition.<Map<String, Object>>builder("options", new HashMap<>()).initializer(HashMap::new).build();
   static final AttributeDefinition<Integer> REQUEST_LIFETIME = AttributeDefinition.builder(Attribute.REQUEST_LIFETIME, 0, Integer.class).build();
   static final AttributeDefinition<Boolean> REQUIRED = AttributeDefinition.builder(Attribute.REQUIRED, false, Boolean.class).build();
   static final AttributeDefinition<Boolean> SERVER = AttributeDefinition.builder(Attribute.SERVER, true, Boolean.class).build();
   static final AttributeDefinition<Boolean> WRAP_GSS_CREDENTIAL = AttributeDefinition.builder(Attribute.WRAP_GSS_CREDENTIAL, false, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KerberosSecurityFactoryConfiguration.class, PRINCIPAL, KEYTAB_PATH, RELATIVE_TO, DEBUG, FAIL_CACHE,
            MECHANISM_NAMES, MECHANISM_OIDS, MINIMUM_REMAINING_LIFETIME, OBTAIN_KERBEROS_TICKET, OPTIONS, REQUEST_LIFETIME,
            REQUIRED, SERVER, WRAP_GSS_CREDENTIAL);
   }

   private final CredentialSource credentialSource;

   KerberosSecurityFactoryConfiguration(AttributeSet attributes, CredentialSource credentialSource) {
      super(Element.KERBEROS, attributes);
      this.credentialSource = credentialSource;
   }

   public String getPrincipal() {
      return attributes.attribute(PRINCIPAL).get();
   }

   public CredentialSource getCredentialSource() {
      return credentialSource;
   }
}
