package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapEntryAlreadyExistsException;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.shaded.com.google.common.io.Files;

public class RemoteLdapServer extends AbstractLdapServer {

   private String initLDIF;

   public RemoteLdapServer(String initLDIF) {
      this.initLDIF = initLDIF;
   }

   private static final Log log = LogFactory.getLog(RemoteLdapServer.class);

   @Override
   public void start(String keystoreFile, File confDir) throws URISyntaxException, LdapException, IOException {
      URI ldapUri = new URI(System.getProperty(TEST_LDAP_URL));
      LdapNetworkConnection connection = new LdapNetworkConnection(ldapUri.getHost(), ldapUri.getPort());
      connection.setTimeOut(60);
      connection.bind(System.getProperty(TEST_LDAP_PRINCIPAL), System.getProperty(TEST_LDAP_CREDENTIAL));

      // for each ldap server, there is a ldif file. you should replace infinispan.ldif content
      String fullPathFile = getClass().getClassLoader().getResource(initLDIF).getFile();
      String fileContent = Files.toString(new File(fullPathFile), Charset.defaultCharset());
      LdifReader ldifReader = new LdifReader();
      List<LdifEntry> entries = ldifReader.parseLdif(fileContent);
      for (LdifEntry ldifEntry : entries) {
         try {
            Entry entry = ldifEntry.getEntry();
            connection.add(entry);
         } catch (LdapEntryAlreadyExistsException e) {
            // for now, remove entry or edit is not supported
            log.debug(ldifEntry.getDn().toString(), e);
         }
      }
   }

   @Override
   public void stop() {
      // it is remote
   }
}
