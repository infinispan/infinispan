package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapEntryAlreadyExistsException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.shaded.com.google.common.io.Files;

public class RemoteLdapServer extends AbstractLdapServer {

   private static final Log log = LogFactory.getLog(RemoteLdapServer.class);

   @Override
   public void start(String keystoreFile, String[] initLDIFs) throws Exception {

      URI ldapUri = new URI(System.getProperty(TEST_LDAP_URL));
      LdapNetworkConnection connection = new LdapNetworkConnection(ldapUri.getHost(), ldapUri.getPort());
      connection.setTimeOut(60);
      connection.bind(System.getProperty(TEST_LDAP_HOST_USER), System.getProperty(TEST_LDAP_HOST_PASSWORD));

      for (String initLDIF : initLDIFs) {
         if (!initLDIF.contains("-dn.ldif")) {
            String fullPathFile = getClass().getClassLoader().getResource(initLDIF).getFile();
            String fileContent = Files.toString(new File(fullPathFile), Charset.defaultCharset());
            String ldif = fileContent
                  .replaceAll("ou=People,dc=infinispan,dc=org", System.getProperty(TEST_LDAP_SEARCH_DN))
                  .replaceAll("ou=Roles,dc=infinispan,dc=org", System.getProperty(TEST_LDAP_SEARCH_DN));
            LdifReader ldifReader = new LdifReader();
            List<LdifEntry> entries = ldifReader.parseLdif(ldif);
            for (LdifEntry ldifEntry : entries) {
               String originalDN = ldifEntry.getDn().toString();
               Entry entry = tranformEntry(ldifEntry.getEntry());
               if (entry == null) {
                  continue;
               }
               try {
                  connection.add(entry);
               } catch (LdapEntryAlreadyExistsException e) {
                  // for now, remove entry or edit is not supported
                  log.debug(originalDN, e);
               }
            }
         }
      }
   }

   protected Entry tranformEntry(Entry entry) {
      return entry;
   }

   @Override
   public void stop() throws Exception {
      // it is remote
   }

   @Override
   public void startKdc() throws IOException, LdapInvalidDnException {
      // it is remote
   }
}
