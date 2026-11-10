package org.infinispan.server.test.core;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Locale;
import java.util.Properties;

import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.ldap.ApacheLdapServer;
import org.infinispan.server.test.core.ldap.Ds389LdapServer;
import org.infinispan.server.test.core.ldap.LdapPorts;
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
      DS389 {
         @Override
         LdapServer ldapServer(boolean withKdc) {
            return new Ds389LdapServer();
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
      int ldapPort = findFreePort();
      int ldapsPort = findFreePort();
      int kdcPort = kdc ? findFreePort() : 0;
      LdapPorts ports = new LdapPorts(ldapPort, ldapsPort, kdcPort);
      ldapServer = LdapServerType.valueOf(type.toUpperCase(Locale.ROOT)).ldapServer(kdc);
      Exceptions.unchecked(() -> {
         ldapServer.start(driver.getCertificateFile("server.pfx").getAbsolutePath(), driver.getConfDir(), ports);
      });
      // Publish the allocated ports so the server realm configuration (and the Kerberos
      // listener) can reach the backend on the ports this test actually bound.
      Properties properties = driver.getConfiguration().properties();
      properties.setProperty(TestSystemPropertyNames.LDAP_PORT, String.valueOf(ldapPort));
      properties.setProperty(TestSystemPropertyNames.LDAPS_PORT, String.valueOf(ldapsPort));
      if (kdcPort > 0) {
         properties.setProperty(TestSystemPropertyNames.KDC_PORT, String.valueOf(kdcPort));
      }
   }

   private static int findFreePort() {
      try (ServerSocket socket = new ServerSocket(0)) {
         return socket.getLocalPort();
      } catch (IOException e) {
         throw new IllegalStateException("Unable to allocate a free port", e);
      }
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
