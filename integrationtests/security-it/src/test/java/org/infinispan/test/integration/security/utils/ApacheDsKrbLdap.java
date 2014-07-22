package org.infinispan.test.integration.security.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.AnnotationUtils;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.cramMD5.CramMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.digestMD5.DigestMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.ntlm.NtlmMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/** 
 * @author vjuranek
 * @since 7.0
 */
public class ApacheDsKrbLdap {
   public static final int LDAP_PORT = 10389;
   public static final int KERBEROS_PORT = 6088;
   public static final String KERBEROS_PRIMARY_REALM = "INFINISPAN.ORG";
   public static final String LDAP_INIT_FILE = "ldif/ispn-krb-test.ldif";
   public static final String BASE_DN = "dc=infinispan,dc=org";

   private static Log log = LogFactory.getLog(ApacheDsKrbLdap.class);
   private DirectoryService directoryService;
   private LdapServer ldapServer;
   private KdcServer kdcServer;

   public ApacheDsKrbLdap(String hostname) throws Exception {
      createDs();
      createKdc();
      createLdap(hostname);
   }
  
   public void start() throws Exception {
      ldapServer.start();
   }
   
   public void stop() throws Exception {
      kdcServer.stop();
      ldapServer.stop();
      directoryService.shutdown();
      FileUtils.deleteDirectory(directoryService.getInstanceLayout().getInstanceDirectory());
   }
   
   @CreateDS(
         name = "InfinispanDS",
         partitions = {
               @CreatePartition(
                     name = "infinispan",
                     suffix = BASE_DN,
                     contextEntry = @ContextEntry(
                           entryLdif =
                           "dn: " + BASE_DN + "\n" +
                           "dc: infinispan\n" +
                           "objectClass: top\n" +
                           "objectClass: domain\n\n" ),
                           indexes = {
                              @CreateIndex( attribute = "objectClass" ),
                              @CreateIndex( attribute = "dc" ),
                              @CreateIndex( attribute = "ou" )
                           }
                     )
         },
         additionalInterceptors = { KeyDerivationInterceptor.class }
   )
   public void createDs() throws Exception {
      directoryService = DSAnnotationProcessor.getDirectoryService();
   }
   
   @CreateKdcServer(
         primaryRealm = KERBEROS_PRIMARY_REALM,
         kdcPrincipal = "krbtgt/" + KERBEROS_PRIMARY_REALM + "@" + KERBEROS_PRIMARY_REALM,
         searchBaseDn = BASE_DN,
         transports = {@CreateTransport( protocol = "UDP", port = KERBEROS_PORT)}
   )
   public void createKdc() throws Exception {
      kdcServer = ServerAnnotationProcessor.getKdcServer(directoryService, KERBEROS_PORT);
   }
   
   @CreateLdapServer(
         transports = { @CreateTransport( protocol = "LDAP",  port = LDAP_PORT) },
         saslRealms = {KERBEROS_PRIMARY_REALM},
         saslMechanisms = {
               @SaslMechanism( name=SupportedSaslMechanisms.GSSAPI, implClass=GssapiMechanismHandler.class),
               @SaslMechanism( name= SupportedSaslMechanisms.PLAIN, implClass=PlainMechanismHandler.class ),
               @SaslMechanism( name=SupportedSaslMechanisms.CRAM_MD5, implClass=CramMd5MechanismHandler.class),
               @SaslMechanism( name= SupportedSaslMechanisms.DIGEST_MD5, implClass=DigestMd5MechanismHandler.class),
               @SaslMechanism( name=SupportedSaslMechanisms.NTLM, implClass=NtlmMechanismHandler.class),
               @SaslMechanism( name=SupportedSaslMechanisms.GSS_SPNEGO, implClass=NtlmMechanismHandler.class)
         }
   )
   public void createLdap(final String hostname) throws Exception {
      final String initFile = System.getProperty("ldap.init.file", LDAP_INIT_FILE);
      final String ldifContent = IOUtils.toString(getClass().getClassLoader().getResource(initFile));
      final SchemaManager schemaManager = directoryService.getSchemaManager();
     
      try {
         for (LdifEntry ldifEntry : new LdifReader(IOUtils.toInputStream(ldifContent))) {
            directoryService.getAdminSession().add(new DefaultEntry(schemaManager, ldifEntry.getEntry()));
         }
      } catch (Exception e) {
         log.error("Error adding ldif entries", e);
         throw e;
      }
      final CreateLdapServer createLdapServer = (CreateLdapServer) AnnotationUtils.getInstance(CreateLdapServer.class);    
      ldapServer = ServerAnnotationProcessor.instantiateLdapServer(createLdapServer, directoryService);
      ldapServer.setSearchBaseDn(BASE_DN);
      ldapServer.setSaslHost(hostname);
      ldapServer.setSaslPrincipal("ldap/" + hostname + "@" + KERBEROS_PRIMARY_REALM);
   }

}
