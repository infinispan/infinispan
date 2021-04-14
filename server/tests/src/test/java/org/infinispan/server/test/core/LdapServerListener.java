package org.infinispan.server.test.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.AnnotationUtils;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.Transport;
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class LdapServerListener implements InfinispanServerListener {
   public static final int KDC_PORT = 6088;
   public static final int LDAP_PORT = 10389;
   public static final int LDAPS_PORT = 10636;
   public static final String DEFAULT_LDIF = "ldif/infinispan.ldif";
   public static final String DOMAIN = "dc=infinispan,dc=org";
   public static final String REALM = "INFINISPAN.ORG";

   private final String initLDIF;
   private DirectoryService directoryService;
   private LdapServer ldapServer;
   private KdcServer kdcServer;
   private boolean withKdc;

   public LdapServerListener() {
      this(DEFAULT_LDIF, false);
   }

   public LdapServerListener(String initLDIF, boolean withKdc) {
      this.initLDIF = initLDIF;
      this.withKdc = withKdc;
   }

   @Override
   public void before(InfinispanServerDriver driver) {
      Exceptions.unchecked(() -> {
         if (withKdc) {
            generateKeyTab(new File(driver.getConfDir(), "hotrod.keytab"), "hotrod/datagrid@INFINISPAN.ORG", "hotrodPassword");
            generateKeyTab(new File(driver.getConfDir(), "http.keytab"), "HTTP/localhost@INFINISPAN.ORG", "httpPassword");
         }
         createDs();
         createLdap(driver);
         ldapServer.start();
         if (withKdc) {
            createKdc();
            kdcServer.start();
         }
      });
   }

   @Override
   public void after(InfinispanServerDriver driver) {
      try {
         if (kdcServer != null) {
            kdcServer.stop();
            kdcServer = null;
         }
         ldapServer.stop();
         directoryService.shutdown();
      } catch (Exception e) {
         // Ignore
      } finally {
         Util.recursiveFileRemove(directoryService.getInstanceLayout().getInstanceDirectory());
      }

      // LdapServer creates an ExecutorFilter with an "unmanaged" executor and doesn't stop the executor itself
      ThreadLeakChecker.ignoreThreadsContaining("pool-.*thread-");
      //
      ThreadLeakChecker.ignoreThreadsContaining("^Thread-\\d+$");
   }

   @CreateDS(
         name = "InfinispanDS",
         partitions = {
               @CreatePartition(
                     name = "infinispan",
                     suffix = DOMAIN,
                     contextEntry = @ContextEntry(
                           entryLdif =
                                 "dn: " + DOMAIN + "\n" +
                                       "dc: infinispan\n" +
                                       "objectClass: top\n" +
                                       "objectClass: domain\n\n"),
                     indexes = {
                           @CreateIndex(attribute = "objectClass"),
                           @CreateIndex(attribute = "dc"),
                           @CreateIndex(attribute = "ou"),
                           @CreateIndex(attribute = "uid"),
                     }
               )
         }
   )
   public void createDs() throws Exception {
      directoryService = DSAnnotationProcessor.getDirectoryService();
      directoryService.getChangeLog().setEnabled(false);
      directoryService.addLast(new KeyDerivationInterceptor());
   }

   @CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = LDAP_PORT, address = "0.0.0.0")})
   public void createLdap(InfinispanServerDriver driver) throws Exception {

      final SchemaManager schemaManager = directoryService.getSchemaManager();


      try (InputStream is = getClass().getClassLoader().getResourceAsStream(initLDIF)) {
         for (LdifEntry ldifEntry : new LdifReader(is)) {
            directoryService.getAdminSession().add(new DefaultEntry(schemaManager, ldifEntry.getEntry()));
         }
      }

      final CreateLdapServer createLdapServer = (CreateLdapServer) AnnotationUtils.getInstance(CreateLdapServer.class);
      ldapServer = ServerAnnotationProcessor.instantiateLdapServer(createLdapServer, directoryService);
      ldapServer.setKeystoreFile(driver.getCertificateFile("server").getAbsolutePath());
      ldapServer.setCertificatePassword(AbstractInfinispanServerDriver.KEY_PASSWORD);
      Transport ldaps = new TcpTransport(LDAPS_PORT);
      ldaps.enableSSL(true);
      ldapServer.addTransports(ldaps);
   }

   private void createKdc() {
      KdcServer kdcServer = new KdcServer();
      kdcServer.setServiceName("TestKDCServer");
      kdcServer.setSearchBaseDn(DOMAIN);
      KerberosConfig config = kdcServer.getConfig();
      config.setServicePrincipal("krbtgt/INFINISPAN.ORG@" + REALM);
      config.setPrimaryRealm(REALM);
      config.setMaximumTicketLifetime(60_000 * 1440);
      config.setMaximumRenewableLifetime(60_000 * 10_080);

      config.setPaEncTimestampRequired(false);

      UdpTransport udp = new UdpTransport("0.0.0.0", KDC_PORT);
      kdcServer.addTransports(udp);

      kdcServer.setDirectoryService(directoryService);
      this.kdcServer = kdcServer;
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
