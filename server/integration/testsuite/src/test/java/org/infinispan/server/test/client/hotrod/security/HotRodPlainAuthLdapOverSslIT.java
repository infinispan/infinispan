package org.infinispan.server.test.client.hotrod.security;

import java.io.File;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.test.integration.security.utils.ApacheDSLdapSSL;
import org.infinispan.test.integration.security.utils.ApacheDsLdap;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer(@RunningServer(name = "hotrodAuthLdapOverSsl"))
public class HotRodPlainAuthLdapOverSslIT extends HotRodSaslAuthTestBase {

   private static final String KEYSTORE_NAME = "keystore_client.jks";
   private static final String KEYSTORE_PASSWORD = "secret";

   private static ApacheDsLdap ldap;

   @InfinispanResource("hotrodAuthLdapOverSsl")
   private RemoteInfinispanServer server;

   @BeforeClass
   public static void kerberosSetup() throws Exception {
      ldap = new ApacheDSLdapSSL("localhost", ITestUtils.SERVER_CONFIG_DIR + File.separator + KEYSTORE_NAME,
            KEYSTORE_PASSWORD);
      ldap.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      ldap.stop();
   }

   @Override
   public String getTestedMech() {
      return "PLAIN";
   }

   @Override
   public RemoteInfinispanServer getRemoteServer() {
      return server;
   }

   @Override
   public void initAsAdmin() {
      initializeOverSsl(ADMIN_LOGIN, ADMIN_PASSWD);
   }

   @Override
   public void initAsReader() {
      initializeOverSsl(READER_LOGIN, READER_PASSWD);
   }

   @Override
   public void initAsWriter() {
      initializeOverSsl(WRITER_LOGIN, WRITER_PASSWD);
   }

   @Override
   public void initAsSupervisor() {
      initializeOverSsl(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD);
   }

}
