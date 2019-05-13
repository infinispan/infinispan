package org.infinispan.server.test.client.hotrod.security;

import java.io.File;
import java.security.PrivilegedActionException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.security.SimpleLoginHandler;
import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({ @RunningServer(name = "hotrodAuthKrb") })
public class HotRodKrbAuthIT extends HotRodSaslAuthTestBase {

   private static final String KRB_REALM = "INFINISPAN.ORG";

   private static ApacheDsKrbLdap krbLdapServer;

   @InfinispanResource("hotrodAuthKrb")
   RemoteInfinispanServer server;

   @BeforeClass
   public static void kerberosSetup() throws Exception {
      krbLdapServer = new ApacheDsKrbLdap("localhost");
      krbLdapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      krbLdapServer.stop();
   }

   @Override
   public RemoteInfinispanServer getRemoteServer() {
      return server;
   }

   protected Subject getSubject(String login, String password) throws LoginException {
      boolean isIBMJDK = System.getProperty("java.vendor").contains("IBM");
      String krbLogin = isIBMJDK ? "/ibm_jaas_krb_login.conf" : "/jaas_krb_login.conf";
      System.setProperty("java.security.auth.login.config", new File(HotRodKrbAuthIT.class.getResource(krbLogin).getFile()).getAbsolutePath());
      System.setProperty("java.security.krb5.conf", new File(HotRodKrbAuthIT.class.getResource("/krb5.conf").getFile()).getAbsolutePath());
      LoginContext lc = new LoginContext("HotRodKrbClient", new SimpleLoginHandler(login + "@" + KRB_REALM, password));
      if (isIBMJDK) {
         // workaround for IBM JDK: the first negotiation always fails, so let's do a dummy login/logout round.
         lc.login();
         lc.logout();
         lc = new LoginContext("HotRodKrbClient", new SimpleLoginHandler(login + "@" + KRB_REALM, password));
      }
      lc.login();
      return lc.getSubject();
   }

   @Override
   public String getTestedMech() {
      return "GSSAPI";
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
