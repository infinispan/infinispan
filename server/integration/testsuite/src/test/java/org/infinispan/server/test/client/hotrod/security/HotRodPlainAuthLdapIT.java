package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.infinispan.test.integration.security.utils.ApacheDsLdap;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer(@RunningServer(name = "hotrodAuthLdap"))
public class HotRodPlainAuthLdapIT extends HotRodSaslAuthTestBase {
   
   private static ApacheDsLdap ldap;
   
   @InfinispanResource("hotrodAuthLdap")
   private RemoteInfinispanServer server;
   
   @BeforeClass
   public static void kerberosSetup() throws Exception {
      ldap = new ApacheDsLdap("localhost");
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
   public String getHRServerHostname() {
      return server.getHotrodEndpoint().getInetAddress().getHostName();
   }

   @Override
   public int getHRServerPort() {
      return server.getHotrodEndpoint().getPort();
   }

   @Override
   public void initAsAdmin() {
      initialize(ADMIN_LOGIN, ADMIN_PASSWD);
   }

   @Override
   public void initAsReader() {
      initialize(READER_LOGIN, READER_PASSWD);
   }

   @Override
   public void initAsWriter() {
      initialize(WRITER_LOGIN, WRITER_PASSWD);
   }

   @Override
   public void initAsSupervisor() {
      initialize(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD);
   }

}
