package org.infinispan.server.test;

import java.io.InputStream;

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
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.Transport;
import org.infinispan.commons.util.Util;
import org.infinispan.test.Exceptions;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class LdapServerRule implements TestRule {
   public static final int LDAP_PORT = 10389;
   public static final int LDAPS_PORT = 10636;
   public static final String DEFAULT_LDIF = "ldif/infinispan.ldif";

   private final String initLDIF;
   private final InfinispanServerRule infinispanServerRule;
   private DirectoryService directoryService;
   private LdapServer ldapServer;

   public LdapServerRule(InfinispanServerRule infinispanServerRule) {
      this(infinispanServerRule, DEFAULT_LDIF);
   }

   public LdapServerRule(InfinispanServerRule infinispanServerRule, String initLDIF) {
      this.infinispanServerRule = infinispanServerRule;
      this.initLDIF = initLDIF;
   }


   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before();
            try {
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before() {
      Exceptions.unchecked(() -> createDs());
      Exceptions.unchecked(() -> createLdap());
      Exceptions.unchecked(() -> ldapServer.start());
   }

   private void after() {
      try {
         ldapServer.stop();
         directoryService.shutdown();
      } catch (Exception e) {
         // Ignore
      } finally {
         Util.recursiveFileRemove(directoryService.getInstanceLayout().getInstanceDirectory());
      }
   }

   @CreateDS(
         name = "InfinispanDS",
         partitions = {
               @CreatePartition(
                     name = "infinispan",
                     suffix = "dc=infinispan,dc=org",
                     contextEntry = @ContextEntry(
                           entryLdif =
                                 "dn: dc=infinispan,dc=org\n" +
                                       "dc: infinispan\n" +
                                       "objectClass: top\n" +
                                       "objectClass: domain\n\n"),
                     indexes = {
                           @CreateIndex(attribute = "objectClass"),
                           @CreateIndex(attribute = "dc"),
                           @CreateIndex(attribute = "ou")
                     }
               )
         }
   )
   public void createDs() throws Exception {
      directoryService = DSAnnotationProcessor.getDirectoryService();
   }

   @CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = LDAP_PORT, address = "0.0.0.0")})
   public void createLdap() throws Exception {

      final SchemaManager schemaManager = directoryService.getSchemaManager();


      try (InputStream is = getClass().getClassLoader().getResourceAsStream(initLDIF)) {
         for (LdifEntry ldifEntry : new LdifReader(is)) {
            directoryService.getAdminSession().add(new DefaultEntry(schemaManager, ldifEntry.getEntry()));
         }
      }

      final CreateLdapServer createLdapServer = (CreateLdapServer) AnnotationUtils.getInstance(CreateLdapServer.class);
      ldapServer = ServerAnnotationProcessor.instantiateLdapServer(createLdapServer, directoryService);
      ldapServer.setKeystoreFile(infinispanServerRule.getServerDriver().getCertificateFile("server").getAbsolutePath());
      ldapServer.setCertificatePassword(InfinispanServerDriver.KEY_PASSWORD);
      Transport ldaps = new TcpTransport(LDAPS_PORT);
      ldaps.enableSSL(true);
      ldapServer.addTransports(ldaps);
   }
}
