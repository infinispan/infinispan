
 package org.infinispan.test.integration.security.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

 /**
 * @author vjuranek
 * @since 7.0
 */
public class ApacheDsLdap {
   
   public static final int LDAP_PORT = 10389;
   public static final String LDAP_INIT_FILE = "ldif/ispn-test.ldif";

   private static Log log = LogFactory.getLog(ApacheDsLdap.class);
   protected DirectoryService directoryService;
   protected LdapServer ldapServer;

   public ApacheDsLdap(String hostname) throws Exception {
      createDs();
      createLdap(hostname);
   }
  
   public void start() throws Exception {
      ldapServer.start();
   }
   
   public void stop() throws Exception {
      ldapServer.stop();
      directoryService.shutdown();
      FileUtils.deleteDirectory(directoryService.getInstanceLayout().getInstanceDirectory());
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
                           "objectClass: domain\n\n" ),
                           indexes = {
                              @CreateIndex( attribute = "objectClass" ),
                              @CreateIndex( attribute = "dc" ),
                              @CreateIndex( attribute = "ou" )
                           }
                     )
         }
   )
   public void createDs() throws Exception {
      directoryService = DSAnnotationProcessor.getDirectoryService();
   }
   
   @CreateLdapServer(transports = { @CreateTransport( protocol = "LDAP",  port = LDAP_PORT) })
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
   }

}
