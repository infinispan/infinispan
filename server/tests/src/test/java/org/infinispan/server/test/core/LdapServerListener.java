package org.infinispan.server.test.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
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
   public static final String[] DEFAULT_LDIF = new String[]{"ldif/infinispan-dn.ldif", "ldif/infinispan.ldif"};

   private final String[] initLDIFs;
   private AbstractLdapServer ldapServer;
   private boolean withKdc;

   public LdapServerListener() {
      this(DEFAULT_LDIF, false);
   }

   public LdapServerListener(String[] initLDIFs, boolean withKdc) {
      this.initLDIFs = initLDIFs;
      this.withKdc = withKdc;
      if ("apache".equals(LDAP_SERVER)) {
         ldapServer = new ApacheLdapServer();
      } else if ("remote".equals(LDAP_SERVER)) {
         ldapServer = new RemoteLdapServer();
      } else {
         throw new IllegalStateException("Unsupported LDAP Server: " + LDAP_SERVER);
      }
   }

   @Override
   public void before(InfinispanServerDriver driver) {
      Exceptions.unchecked(() -> {
         if (withKdc) {
            generateKeyTab(new File(driver.getConfDir(), "hotrod.keytab"), "hotrod/datagrid@INFINISPAN.ORG", "hotrodPassword");
            generateKeyTab(new File(driver.getConfDir(), "http.keytab"), "HTTP/localhost@INFINISPAN.ORG", "httpPassword");
         }
         ldapServer.start(driver.getCertificateFile("server").getAbsolutePath(), this.initLDIFs);
         if (withKdc) {
            ldapServer.startKdc();
         }
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

   public static String generateKeyTab(File keyTabFile, String... credentials) {
      List<KeytabEntry> entries = new ArrayList<>();
      KerberosTime ktm = new KerberosTime();

      for (int i = 0; i < credentials.length; ) {
         String principal = credentials[i++];
         String password = credentials[i++];

         for (Map.Entry<EncryptionType, EncryptionKey> keyEntry : KerberosKeyFactory.getKerberosKeys(principal, password)
               .entrySet()) {
            EncryptionKey key = keyEntry.getValue();
            entries.add(new KeytabEntry(principal, KerberosPrincipal.KRB_NT_PRINCIPAL, ktm, (byte) key.getKeyVersion(), key));
         }
      }

      Keytab keyTab = Keytab.getInstance();
      keyTab.setEntries(entries);
      try {
         keyTab.write(keyTabFile);
         return keyTabFile.getAbsolutePath();
      } catch (IOException e) {
         throw new IllegalStateException("Cannot create keytab: " + keyTabFile, e);
      }
   }
}
