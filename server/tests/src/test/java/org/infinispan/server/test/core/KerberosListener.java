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
      try {
         String s = Files.readString(krb5conf);
         s = s.replaceAll("localhost", address.getHostAddress());
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
