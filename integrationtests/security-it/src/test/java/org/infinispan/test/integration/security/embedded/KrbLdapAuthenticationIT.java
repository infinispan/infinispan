
 package org.infinispan.test.integration.security.embedded;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.infinispan.test.integration.security.utils.Deployments;
import org.infinispan.test.integration.security.utils.SimplePrincipalGroupRoleMapper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class KrbLdapAuthenticationIT extends AbstractAuthentication {

   public static final String ADMIN_ROLE = "AdminIspnRole";
   public static final String WRITER_ROLE = "WriterIspnRole";
   public static final String READER_ROLE = "ReaderIspnRole";
   public static final String UNPRIVILEGED_ROLE = "UnprivilegedIspnRole";
   
   private static ApacheDsKrbLdap krbLdapServer;
   
   @BeforeClass
   public static void ldapSetup() throws Exception {
      System.setProperty("java.security.krb5.conf", KrbLdapAuthenticationIT.class.getResource("/krb5.conf").getPath());
      krbLdapServer = new ApacheDsKrbLdap("localhost");
      krbLdapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      krbLdapServer.stop();
   }

   @Deployment
   public static WebArchive getDeployment() {
      return Deployments.createKrbLdapTestDeployment();
   }

   public Map<String, AuthorizationPermission[]> getRolePermissionMap() {
      Map<String, AuthorizationPermission[]> roles = new HashMap<String, AuthorizationPermission[]>();
      roles.put(ADMIN_ROLE, new AuthorizationPermission[] { AuthorizationPermission.ALL });
      roles.put(WRITER_ROLE, new AuthorizationPermission[] { AuthorizationPermission.WRITE });
      roles.put(READER_ROLE, new AuthorizationPermission[] { AuthorizationPermission.READ });
      roles.put(UNPRIVILEGED_ROLE, new AuthorizationPermission[] { AuthorizationPermission.NONE });
      return roles;
   }

   public PrincipalRoleMapper getPrincipalRoleMapper() {
      return new SimplePrincipalGroupRoleMapper();
   }
   
   public String getSecurityDomainName() {
      return null; //not used in this test
   }
   
   public Subject getAdminSubject() throws LoginException {
      return authenticateWithKrb("ispn-admin");
   }

   public Subject getWriterSubject() throws LoginException {
      return authenticateWithKrb("ispn-writer");
   }
   
   public Subject getReaderSubject() throws LoginException {
      return authenticateWithKrb("ispn-reader");
   }
   
   public Subject getUnprivilegedSubject() throws LoginException {
      return authenticateWithKrb("ispn-unprivileged");
   }

}
