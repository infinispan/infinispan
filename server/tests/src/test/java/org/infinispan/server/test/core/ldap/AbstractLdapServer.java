package org.infinispan.server.test.core.ldap;

import java.io.File;

public abstract class AbstractLdapServer {

   public static final String TEST_LDAP_URL = "org.infinispan.test.ldap.url";
   public static final String TEST_LDAP_PRINCIPAL = "org.infinispan.test.ldap.principal";
   public static final String TEST_LDAP_CREDENTIAL = "org.infinispan.test.ldap.credential";

   public abstract void start(String keystoreFile, File confDir) throws Exception;

   public abstract void stop() throws Exception;
}
