package org.infinispan.server.test.core.ldap;

import java.io.IOException;

import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;

public abstract class AbstractLdapServer {

   public abstract void start(String keystoreFile, String[] initLDIFs) throws Exception;

   public abstract void stop() throws Exception;

   public abstract void startKdc() throws IOException, LdapInvalidDnException;
}
