package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.DistributedSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class DistributedRealmConfiguration extends ConfigurationElement<DistributedRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", "distributed", String.class).build();
   static final AttributeDefinition<List<String>> REALMS = AttributeDefinition.builder("realms", null, (Class<List<String>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(DistributedRealmConfiguration.class, NAME, REALMS);
   }

   DistributedRealmConfiguration(AttributeSet attributes) {
      super(Element.DISTRIBUTED_REALM, attributes);
   }

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      domainBuilder.setDefaultRealmName(name()); // We make this the default realm
      List<String> names = realms();
      SecurityRealm[] securityRealms;
      if (names.isEmpty()) { // we add all realms
         securityRealms = realm.realms.values().toArray(new SecurityRealm[0]);
      } else { // only the specified realms
         securityRealms = new SecurityRealm[names.size()];
         for (int i = 0; i < names.size(); i++) {
            SecurityRealm securityRealm = realm.realms.get(names.get(i));
            if (securityRealm == null) {
               throw Server.log.unknownRealm(names.get(i));
            } else {
               securityRealms[i] = securityRealm;
            }
         }
      }
      return new DistributedSecurityRealm(securityRealms);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public List<String> realms() {
      return attributes.attribute(REALMS).get();
   }
}
