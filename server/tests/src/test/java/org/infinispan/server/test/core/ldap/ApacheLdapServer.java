package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.exception.LdapException;
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
import org.apache.directory.server.core.api.CoreSession;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.Transport;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.admin.kadmin.local.LocalKadmin;
import org.apache.kerby.kerberos.kerb.admin.kadmin.local.LocalKadminImpl;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.server.KdcConfig;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.KdcServer;
import org.apache.kerby.kerberos.kerb.type.KerberosTime;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionKey;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionType;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AbstractInfinispanServerDriver;

public class ApacheLdapServer implements LdapServer {

   private static final String LDAP_HOST = "0.0.0.0";
   public static final int KDC_PORT = 6088;
   public static final int LDAP_PORT = 10389;
   public static final int LDAPS_PORT = 10636;
   public static final String DOMAIN = "dc=infinispan,dc=org";
   public static final String REALM = "INFINISPAN.ORG";

   private DirectoryService directoryService;
   private org.apache.directory.server.ldap.LdapServer ldapServer;
   private KdcServer kdcServer;
   private final boolean withKdc;
   private final String initLDIF;
   private LocalKadmin kadmin;

   public ApacheLdapServer(boolean withKdc, String initLDIF) {
      this.withKdc = withKdc;
      this.initLDIF = initLDIF;
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

   @CreateLdapServer(
         transports = {
               @CreateTransport(protocol = "LDAP", port = LDAP_PORT, address = LDAP_HOST),
               @CreateTransport(protocol = "LDAPS", port = LDAPS_PORT, address = LDAP_HOST, ssl = true),
         }
   )
   public void createLdap(String keystoreFile) throws Exception {
      final CreateLdapServer createLdapServer = (CreateLdapServer) AnnotationUtils.getInstance(CreateLdapServer.class);
      ldapServer = ServerAnnotationProcessor.instantiateLdapServer(createLdapServer, directoryService);
      ldapServer.setKeystoreFile(keystoreFile);
      ldapServer.setCertificatePassword(AbstractInfinispanServerDriver.KEY_PASSWORD);
      Arrays.stream(ldapServer.getTransports())
            .filter(Transport::isSSLEnabled)
            .map(t -> (TcpTransport) t)
            .forEach(t -> t.setEnabledProtocols(Arrays.asList("TLSv1.3", "TLSv1.2")));
   }

   @Override
   public void start(String keystoreFile, File confDir) throws Exception {
      createDs();
      createLdap(keystoreFile);
      ldapServer.start();
      if (withKdc) {
         startKdc();
      }
      loadLDIF(confDir);
   }

   @Override
   public void stop() throws Exception {
      try {
         if (kdcServer != null) {
            kdcServer.stop();
            kdcServer = null;
         }
         ldapServer.stop();
         directoryService.shutdown();
      } finally {
         Util.recursiveFileRemove(directoryService.getInstanceLayout().getInstanceDirectory());
      }
   }

   private void loadLDIF(File confDir) throws IOException, LdapException, KrbException {
      final SchemaManager schemaManager = directoryService.getSchemaManager();
      CoreSession session = directoryService.getAdminSession();
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(initLDIF)) {
         for (LdifEntry ldifEntry : new LdifReader(is)) {
            session.add(new DefaultEntry(schemaManager, ldifEntry.getEntry()));
            Attribute attribute = ldifEntry.get("krb5PrincipalName");
            if (attribute != null && kadmin != null) {
               String krb5PrincipalName = attribute.getString();
               String password = ldifEntry.get("userPassword").getString();
               kadmin.addPrincipal(krb5PrincipalName, password);
               if (krb5PrincipalName.contains("/")) {
                  String name = krb5PrincipalName.substring(0, krb5PrincipalName.indexOf('/')).toLowerCase();
                  generateKeyTab(new File(confDir, name + ".keytab"), krb5PrincipalName, password);
               }
            }
         }
      }
   }

   private void startKdc() throws KrbException {
      createKdc();
      kdcServer.init();
      kadmin = new LocalKadminImpl(kdcServer.getKdcSetting(), kdcServer.getIdentityService());
      kdcServer.start();
   }

   private void createKdc() {
      kdcServer = new KdcServer();
      kdcServer.setKdcRealm(REALM);
      kdcServer.setAllowUdp(true);
      kdcServer.setKdcPort(KDC_PORT);
      kdcServer.setKdcHost(LDAP_HOST);
      KdcConfig config = kdcServer.getKdcConfig();
      config.setString(KdcConfigKey.KDC_SERVICE_NAME, "TestKDCServer");
      config.setLong(KdcConfigKey.MAXIMUM_TICKET_LIFETIME, 60_000L * 1440);
      config.setLong(KdcConfigKey.MAXIMUM_RENEWABLE_LIFETIME, 60_000L * 10_080);
      config.setBoolean(KdcConfigKey.PA_ENC_TIMESTAMP_REQUIRED, false);
   }

   public static void generateKeyTab(File keyTabFile, String... credentials) {
      List<KeytabEntry> entries = new ArrayList<>();
      KerberosTime ktm = new KerberosTime();

      for (int i = 0; i < credentials.length; ) {
         String principal = credentials[i++];
         String password = credentials[i++];

         for (Map.Entry<org.apache.directory.shared.kerberos.codec.types.EncryptionType, org.apache.directory.shared.kerberos.components.EncryptionKey> entry : KerberosKeyFactory.getKerberosKeys(principal, password)
               .entrySet()) {
            EncryptionType type = EncryptionType.fromValue(entry.getKey().getValue());
            EncryptionKey xkey = new EncryptionKey(type, entry.getValue().getKeyValue());
            entries.add(new KeytabEntry(new PrincipalName(principal), ktm, (byte) entry.getValue().getKeyVersion(), xkey));
         }
      }
      Keytab keyTab = new Keytab();
      keyTab.addKeytabEntries(entries);
      try {
         keyTab.store(keyTabFile);
      } catch (IOException e) {
         throw new IllegalStateException("Cannot create keytab: " + keyTabFile, e);
      }
   }
}
