package org.infinispan.server.test.core;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KerberosListener implements InfinispanServerListener {
   private String oldKrb5Conf;

   @Override
   public void before(InfinispanServerDriver driver) {
      Path krb5conf = Paths.get(System.getProperty("build.directory"), "test-classes", "configuration/krb5.conf");
      InetAddress address = driver.getTestHostAddress();
      String kdcPort = driver.getConfiguration().properties().getProperty(TestSystemPropertyNames.KDC_PORT);
      if (kdcPort == null) {
         throw new IllegalStateException("No KDC port allocated; ensure LdapServerListener(true) is registered before KerberosListener");
      }
      try {
         String s = Files.readString(krb5conf);
         s = s.replaceAll("localhost", address.getHostAddress());
         // The KDC port is allocated per test; rewrite the port on the kdc line, preserving the host.
         s = s.replaceAll("(kdc\\s*=\\s*[^\\s:]+):\\d+", "$1:" + kdcPort);
         Files.writeString(krb5conf, s);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      oldKrb5Conf = System.setProperty("java.security.krb5.conf", krb5conf.toString());
   }

   @Override
   public void after(InfinispanServerDriver driver) {
      if (oldKrb5Conf != null) {
         System.setProperty("java.security.krb5.conf", oldKrb5Conf);
      } else {
         System.clearProperty("java.security.krb5.conf");
      }
   }
}
