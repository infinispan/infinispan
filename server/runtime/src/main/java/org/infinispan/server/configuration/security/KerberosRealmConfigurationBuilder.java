package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.RELATIVE_TO;
import static org.infinispan.server.configuration.security.KerberosRealmConfiguration.KEYTAB_PATH;

import java.io.File;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.security.realm.KerberosSecurityRealm;

/**
 * @since 10.0
 */
public class KerberosRealmConfigurationBuilder implements Builder<KerberosRealmConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;
   private KerberosSecurityRealm securityRealm;

   KerberosRealmConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = KerberosRealmConfiguration.attributeDefinitionSet();
   }

   public KerberosRealmConfigurationBuilder path(String path) {
      attributes.attribute(KEYTAB_PATH).set(path);
      return this;
   }

   public KerberosRealmConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(KerberosRealmConfiguration.RELATIVE_TO).set(relativeTo);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public KerberosRealmConfiguration create() {
      return new KerberosRealmConfiguration(attributes.protect(), securityRealm);
   }

   @Override
   public KerberosRealmConfigurationBuilder read(KerberosRealmConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   public KerberosSecurityRealm build() {
      if (securityRealm == null) {
         String path = attributes.attribute(KEYTAB_PATH).get();
         String relativeTo = attributes.attribute(RELATIVE_TO).get();
         File keyTab = new File(ParseUtils.resolvePath(path, relativeTo));
         this.securityRealm = new KerberosSecurityRealm(keyTab);
         realmBuilder.domainBuilder().addRealm("kerberos", securityRealm).build();
      }
      return securityRealm;
   }
}
