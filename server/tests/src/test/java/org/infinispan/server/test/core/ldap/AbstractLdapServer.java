package org.infinispan.server.test.core.ldap;

import java.io.IOException;

import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;

public abstract class AbstractLdapServer {

   public static final String TEST_LDAP_URL = "org.infinispan.test.ldap.url";
   public static final String TEST_LDAP_PRINCIPAL = "org.infinispan.test.ldap.principal";
   public static final String TEST_LDAP_CREDENTIAL = "org.infinispan.test.ldap.credential";

   public abstract void start(String keystoreFile, String initLDIF) throws Exception;

   public abstract void stop() throws Exception;

   public abstract void startKdc() throws IOException, LdapInvalidDnException;
}
