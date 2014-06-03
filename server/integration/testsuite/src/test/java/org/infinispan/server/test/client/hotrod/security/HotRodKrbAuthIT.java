package org.infinispan.server.test.client.hotrod.security;

import java.security.PrivilegedActionException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.infinispan.server.test.category.Security;
import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({ Security.class })
public class HotRodKrbAuthIT extends HotRodSaslAuthTestBase {

   private static final String ARQ_CONTAINER_ID = "hotrodAuthKrb";
   private static final String KRB_REALM = "INFINISPAN.ORG";

   private static ApacheDsKrbLdap krbLdapServer;

   @ArquillianResource
   public ContainerController controller;

   @BeforeClass
   public static void kerberosSetup() throws Exception {
      krbLdapServer = new ApacheDsKrbLdap("localhost");
      krbLdapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      krbLdapServer.stop();
   }

   @Before
   public void startIspnServer() {
      controller.start(ARQ_CONTAINER_ID);
   }

   @After
   public void stopIspnServer() {
      controller.stop(ARQ_CONTAINER_ID);
   }

   protected Subject getSubject(String login, String password) throws LoginException {
      System.setProperty("java.security.auth.login.config", HotRodKrbAuthIT.class.getResource("/jaas_krb_login.conf")
            .getPath());
      System.setProperty("java.security.krb5.conf", HotRodKrbAuthIT.class.getResource("/krb5.conf").getPath());
      LoginContext lc = new LoginContext("HotRodKrbClient", new LoginHandler(login + "@" + KRB_REALM, password));
      lc.login();
      return lc.getSubject();
   }

   @Override
   public String getTestedMech() {
      return "GSSAPI";
   }

   @Override
   public String getHRServerHostname() {
      return "localhost";
   }

   @Override
   public int getHRServerPort() {
      return 11222;
   }

   @Override
   public void initAsAdmin() throws PrivilegedActionException, LoginException {
      initialize(getSubject(ADMIN_LOGIN, ADMIN_PASSWD));
   }

   @Override
   public void initAsReader() throws PrivilegedActionException, LoginException {
      initialize(getSubject(READER_LOGIN, READER_PASSWD));
   }

   @Override
   public void initAsWriter() throws PrivilegedActionException, LoginException {
      initialize(getSubject(WRITER_LOGIN, WRITER_PASSWD));
   }

   @Override
   public void initAsSupervisor() throws PrivilegedActionException, LoginException {
      initialize(getSubject(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD));
   }

}
