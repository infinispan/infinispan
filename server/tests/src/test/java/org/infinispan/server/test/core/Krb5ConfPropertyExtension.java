package org.infinispan.server.test.core;

import java.nio.file.Paths;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class Krb5ConfPropertyExtension implements AfterAllCallback, BeforeAllCallback {

   private static String oldKrb5Conf;
   @Override
   public void afterAll(ExtensionContext context) {
      oldKrb5Conf = System.setProperty("java.security.krb5.conf", Paths.get("src/test/resources/configuration/krb5.conf").toString());
   }

   @Override
   public void beforeAll(ExtensionContext context) {
      if (oldKrb5Conf != null) {
         System.setProperty("java.security.krb5.conf", oldKrb5Conf);
      }
   }
}
