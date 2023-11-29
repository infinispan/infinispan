package org.infinispan.server.test.core.ldap;

import java.io.File;

public interface LdapServer {

   String TEST_LDAP_URL = "org.infinispan.test.ldap.url";
   String TEST_LDAP_PRINCIPAL = "org.infinispan.test.ldap.principal";
   String TEST_LDAP_CREDENTIAL = "org.infinispan.test.ldap.credential";

   void start(String keystoreFile, File confDir) throws Exception;

   void stop() throws Exception;
}
