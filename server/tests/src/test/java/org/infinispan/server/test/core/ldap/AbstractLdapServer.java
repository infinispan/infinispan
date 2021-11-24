package org.infinispan.server.test.core.ldap;

import java.io.IOException;

import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;

public abstract class AbstractLdapServer {
   public static final String TEST_LDAP_URL = "org.infinispan.test.ldap.url";
   public static final String TEST_LDAP_PRINCIPAL = "org.infinispan.test.ldap.principal";
   public static final String TEST_LDAP_HOST_USER = "org.infinispan.test.ldap.host.user";
   public static final String TEST_LDAP_HOST_PASSWORD = "org.infinispan.test.ldap.host.password";
   public static final String TEST_LDAP_SEARCH_DN = "org.infinispan.test.ldap.search-dn";
   public static final String TEST_LDAP_ATTRIBUTE_TO = "org.infinispan.test.ldap.identity-mapping.attribute.to";
   public static final String TEST_LDAP_FILTER_DN = "org.infinispan.test.ldap.filter-dn";

   public abstract void start(String keystoreFile, String[] initLDIFs) throws Exception;

   public abstract void stop() throws Exception;

   public abstract void startKdc() throws IOException, LdapInvalidDnException;
}
