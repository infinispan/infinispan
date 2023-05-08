package org.infinispan.quarkus.server.runtime.graal;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.configuration.security.LdapRealmConfigurationBuilder;
import org.infinispan.server.configuration.security.TrustStoreRealmConfigurationBuilder;
import org.infinispan.util.logging.Log;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

public class FixModuleClasses {
}

@Delete
@TargetClass(org.wildfly.security.manager.action.GetModuleClassLoaderAction.class)
final class Delete_orgwildflysecuritymanageractionGetModuleClassLoaderAction { }

@TargetClass(ServerConfigurationParser.class)
final class Target_ServerConfigurationParser {
   @Alias
   private static Log coreLog;

   @Substitute
   private void parseLdapRealm(ConfigurationReader reader, ServerConfigurationBuilder builder, LdapRealmConfigurationBuilder ldapRealmConfigBuilder) {
      coreLog.debug("LDAP Realm is not supported in native mode - ignoring element");
      // Just read until end of token
      while (reader.inTag()) {

      }
   }

   @Substitute
   private void parseTrustStoreRealm(ConfigurationReader reader, TrustStoreRealmConfigurationBuilder trustStoreBuilder) {
      coreLog.debug("TrustStore Realm is not supported in native mode - ignoring element");
      // Just read until end of token
      while (reader.inTag()) {

      }
   }
}
