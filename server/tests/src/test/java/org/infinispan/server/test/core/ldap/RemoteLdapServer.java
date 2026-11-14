package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;

public class RemoteLdapServer implements LdapServer {

   private final String initLDIF;

   public RemoteLdapServer(String initLDIF) {
      this.initLDIF = initLDIF;
   }

   @Override
   public void start(String keystoreFile, File confDir, LdapPorts ports) throws URISyntaxException, LdapException, IOException {
      URI ldapUri = new URI(System.getProperty(TEST_LDAP_URL));
      try (LdapNetworkConnection connection = new LdapNetworkConnection(ldapUri.getHost(), ldapUri.getPort())) {
         connection.setTimeOut(60);
         connection.bind(System.getProperty(TEST_LDAP_PRINCIPAL), System.getProperty(TEST_LDAP_CREDENTIAL));

         // for each LDAP server there is an LDIF file. you should replace infinispan.ldif content
         try (InputStream ldif = getClass().getClassLoader().getResourceAsStream(initLDIF)) {
            LdifImporter.importEntries(connection, ldif);
         }
      }
   }

   @Override
   public void stop() {
      // it is remote
   }
}
