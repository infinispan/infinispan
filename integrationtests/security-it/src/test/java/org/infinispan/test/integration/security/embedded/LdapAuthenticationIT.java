package org.infinispan.test.integration.security.embedded;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.integration.security.utils.ApacheDsLdap;
import org.infinispan.test.integration.security.utils.Deployments;
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
public class LdapAuthenticationIT extends AbstractAuthentication {

   public static final String SECURITY_DOMAIN_NAME = "ispn-secure";
   public static final String ADMIN_ROLE = "admin";
   public static final String ADMIN_PASSWD = "strongPassword";
   public static final String WRITER_ROLE = "writer";
   public static final String WRITER_PASSWD = "somePassword";
   public static final String READER_ROLE = "reader";
   public static final String READER_PASSWD = "password";
   public static final String UNPRIVILEGED_ROLE = "unprivileged";
   public static final String UNPRIVILEGED_PASSWD = "weakPassword";

   private static ApacheDsLdap ldapServer;

   @BeforeClass
   public static void ldapSetup() throws Exception {
      ldapServer = new ApacheDsLdap("localhost");
      ldapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      ldapServer.stop();
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
      return new IdentityRoleMapper();
   }
   
   public String getSecurityDomainName() {
      return SECURITY_DOMAIN_NAME;
   }

   public Subject getAdminSubject() throws LoginException {
      return authenticate(ADMIN_ROLE, ADMIN_PASSWD);
   }

   public Subject getWriterSubject() throws LoginException {
      return authenticate(WRITER_ROLE, WRITER_PASSWD);
   }

   public Subject getReaderSubject() throws LoginException {
      return authenticate(READER_ROLE, READER_PASSWD);
   }

   public Subject getUnprivilegedSubject() throws LoginException {
      return authenticate(UNPRIVILEGED_ROLE, UNPRIVILEGED_PASSWD);
   }

}
