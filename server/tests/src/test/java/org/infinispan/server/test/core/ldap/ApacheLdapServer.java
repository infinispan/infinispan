package org.infinispan.server.test.core.ldap;

import java.io.IOException;
import java.io.InputStream;

import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
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
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.Transport;
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.AbstractInfinispanServerDriver;

public class ApacheLdapServer extends AbstractLdapServer {

   private static final String LDAP_HOST = "0.0.0.0";
   public static final int KDC_PORT = 6088;
   public static final int LDAP_PORT = 10389;
   public static final int LDAPS_PORT = 10636;
   public static final String DOMAIN = "dc=infinispan,dc=org";
   public static final String REALM = "INFINISPAN.ORG";

   private DirectoryService directoryService;
   private LdapServer ldapServer;
   private KdcServer kdcServer;

   public ApacheLdapServer() {
   }

   @CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = LDAP_PORT, address = LDAP_HOST)})
   public void createLdap(String keystoreFile, String initLDIF) throws Exception {

      final SchemaManager schemaManager = directoryService.getSchemaManager();

      try (InputStream is = getClass().getClassLoader().getResourceAsStream(initLDIF)) {
         for (LdifEntry ldifEntry : new LdifReader(is)) {
            directoryService.getAdminSession().add(new DefaultEntry(schemaManager, ldifEntry.getEntry()));
         }
      }

      final CreateLdapServer createLdapServer = (CreateLdapServer) AnnotationUtils.getInstance(CreateLdapServer.class);

      ldapServer = ServerAnnotationProcessor.instantiateLdapServer(createLdapServer, directoryService);
      ldapServer.setKeystoreFile(keystoreFile);
      ldapServer.setCertificatePassword(AbstractInfinispanServerDriver.KEY_PASSWORD);
      Transport ldaps = new TcpTransport(LDAPS_PORT);
      ldaps.enableSSL(true);
      ldapServer.addTransports(ldaps);
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

   @Override
   public void start(String keystoreFile, String initLDIF) throws Exception {
      createDs();
      createLdap(keystoreFile, initLDIF);
      ldapServer.start();
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

   @Override
   public void startKdc() throws IOException, LdapInvalidDnException {
      createKdc();
      kdcServer.start();
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
}
