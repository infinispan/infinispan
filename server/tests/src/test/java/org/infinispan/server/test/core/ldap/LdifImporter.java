package org.infinispan.server.test.core.ldap;

import java.io.IOException;
import java.io.InputStream;

import org.apache.directory.api.ldap.model.exception.LdapEntryAlreadyExistsException;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Adds the entries of an LDIF stream to an LDAP server over a bound {@link LdapNetworkConnection}, skipping any entry
 * whose DN already exists so that seeding is idempotent. Shared by {@link RemoteLdapServer} and {@link Ds389LdapServer}.
 */
public final class LdifImporter {

   private static final Log log = LogFactory.getLog(LdifImporter.class);

   /** No instances. */
   private LdifImporter() {
      // no instances
   }

   public static void importEntries(LdapNetworkConnection connection, InputStream ldif) throws IOException, LdapException {
      try (LdifReader reader = new LdifReader(ldif)) {
         for (LdifEntry entry : reader) {
            try {
               connection.add(entry.getEntry());
            } catch (LdapEntryAlreadyExistsException e) {
               // seeding must be idempotent: skip entries whose DN already exists
               log.debug("Skipping existing LDAP entry " + entry.getDn(), e);
            }
         }
      }
   }
}
