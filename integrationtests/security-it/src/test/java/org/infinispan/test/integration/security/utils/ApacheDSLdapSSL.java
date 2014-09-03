
 package org.infinispan.test.integration.security.utils;

import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.Transport;

/** 
 * @author vjuranek
 * @since 7.0
 */
public class ApacheDSLdapSSL extends ApacheDsLdap {
   
   public static final int LDAPS_PORT = 10636;
   
   public ApacheDSLdapSSL(String hostname, String keystorePath, String keystorePasswd) throws Exception {
      super(hostname);
      addLdaps(keystorePath, keystorePasswd);
   }
   
   
   public void addLdaps(final String keystorePath, final String keystorePasswd) throws Exception {
      Transport ldaps = new TcpTransport(LDAPS_PORT);
      ldaps.enableSSL(true);
      ldapServer.addTransports(ldaps);
      ldapServer.setKeystoreFile(keystorePath);
      ldapServer.setCertificatePassword(keystorePasswd);
   }

}
