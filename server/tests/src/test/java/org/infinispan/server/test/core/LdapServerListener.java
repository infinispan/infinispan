package org.infinispan.server.test.core;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.server.test.core.ldap.AbstractLdapServer;
import org.infinispan.server.test.core.ldap.ApacheLdapServer;
import org.infinispan.server.test.core.ldap.RemoteLdapServer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class LdapServerListener implements InfinispanServerListener {
   public static final String LDAP_SERVER = System.getProperty(TestSystemPropertyNames.LDAP_SERVER, "apache");
   private static final String DEFAULT_LDIF = "ldif/infinispan.ldif";
   private static final String KERBEROS_LDIF = "ldif/infinispan-kerberos.ldif";

   private AbstractLdapServer ldapServer;

   public LdapServerListener() {
      this(false);
   }

   public LdapServerListener(boolean withKdc) {
      if ("apache".equals(LDAP_SERVER)) {
         ldapServer = new ApacheLdapServer(withKdc, withKdc ? KERBEROS_LDIF : DEFAULT_LDIF);
      // when using the remote ldap server, you should overwrite the content of the ldif files before running the test
      } else if ("remote".equals(LDAP_SERVER)) {
         ldapServer = new RemoteLdapServer(withKdc ? KERBEROS_LDIF : DEFAULT_LDIF);
      } else {
         throw new IllegalStateException("Unsupported LDAP Server: " + LDAP_SERVER);
      }
   }

   @Override
   public void before(InfinispanServerDriver driver) {
      Exceptions.unchecked(() -> {
         ldapServer.start(driver.getCertificateFile("server.pfx").getAbsolutePath(), driver.getConfDir());
      });
   }

   @Override
   public void after(InfinispanServerDriver driver) {
      try {
         ldapServer.stop();
      } catch (Exception e) {
         // Ignore
      }

      // LdapServer creates an ExecutorFilter with an "unmanaged" executor and doesn't stop the executor itself
      ThreadLeakChecker.ignoreThreadsContaining("pool-.*thread-");
      //
      ThreadLeakChecker.ignoreThreadsContaining("^Thread-\\d+$");
   }
}
