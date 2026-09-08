package org.infinispan.server.test.core;

import java.util.Locale;

import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.ldap.ApacheLdapServer;
import org.infinispan.server.test.core.ldap.LdapServer;
import org.infinispan.server.test.core.ldap.RemoteLdapServer;
import org.infinispan.server.test.core.ldap.SambaLdapServer;
import org.infinispan.testing.Exceptions;
import org.infinispan.testing.ThreadLeakChecker;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class LdapServerListener implements InfinispanServerListener {
   private static final String DEFAULT_LDIF = "ldif/infinispan.ldif";
   private static final String KERBEROS_LDIF = "ldif/infinispan-kerberos.ldif";
   private final boolean kdc;

   public enum LdapServerType {
      APACHE {
         @Override
         LdapServer ldapServer(boolean withKdc) {
            return new ApacheLdapServer(withKdc, withKdc ? KERBEROS_LDIF : DEFAULT_LDIF);
         }
      },
      SAMBA {
         @Override
         LdapServer ldapServer(boolean withKdc) {
            return new SambaLdapServer();
         }
      },
      REMOTE {
         @Override
         LdapServer ldapServer(boolean withKdc) {
            return new RemoteLdapServer(withKdc ? KERBEROS_LDIF : DEFAULT_LDIF);
         }
      };

      abstract LdapServer ldapServer(boolean withKdc);
   }

   private LdapServer ldapServer;

   public LdapServerListener() {
      this(false);
   }

   public LdapServerListener(boolean withKdc) {
      this.kdc =withKdc;
   }

   @Override
   public void before(InfinispanServerDriver driver) {
      String type = driver.getConfiguration().properties().getProperty(TestSystemPropertyNames.LDAP_SERVER, "apache");
      ldapServer = LdapServerType.valueOf(type.toUpperCase(Locale.ROOT)).ldapServer(kdc);
      Exceptions.unchecked(() -> {
         ldapServer.start(driver.getCertificateFile("server.pfx").getAbsolutePath(), driver.getConfDir());
      });
   }

   @Override
   public void after(InfinispanServerDriver driver) {
      Util.close(ldapServer);
      // LdapServer creates an ExecutorFilter with an "unmanaged" executor and doesn't stop the executor itself
      ThreadLeakChecker.ignoreThreadsContaining("pool-.*thread-");
      //
      ThreadLeakChecker.ignoreThreadsContaining("^Thread-\\d+$");
   }
}
