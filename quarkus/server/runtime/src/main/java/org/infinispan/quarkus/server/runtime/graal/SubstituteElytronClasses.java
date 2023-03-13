package org.infinispan.quarkus.server.runtime.graal;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.configuration.security.LdapRealmConfiguration;
import org.infinispan.server.configuration.security.RealmConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.security.ElytronJMXAuthenticator;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;
import org.wildfly.security.auth.server.ModifiableRealmIdentity;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import java.security.Principal;
import java.util.Properties;
public class SubstituteElytronClasses {
}

@TargetClass(LdapRealmConfiguration.class)
final class Target_LdapRealmConfiguration {
   @Substitute
   public SecurityRealm build(SecurityConfiguration security, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      return LdapSecurityRealmBuilder.builder().build();
   }
}

@TargetClass(ElytronJMXAuthenticator.class)
final class Target_ElytronJMXAuthenticator {
   @Substitute
   public static void init(ServerConfiguration serverConfiguration) {
      // no-op
   }

   @Substitute
   public boolean test(CallbackHandler callbackHandler, Subject subject) {
      return false;
   }
}

@TargetClass(className = "org.wildfly.security.auth.realm.ldap.LdapSecurityRealm")
final class Target_LdapSecurityRealm {

   @Substitute
   public RealmIdentity getRealmIdentity(Principal principal) {
      return null;
   }

   @Substitute
   public ModifiableRealmIdentity getRealmIdentityForUpdate(Principal principal) {
      return null;
   }
}
