package org.infinispan.test.integration.security.embedded;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.integration.security.tasks.AbstractSecurityDomainsServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractTraceLoggingServerSetupTask;
import org.infinispan.test.integration.security.utils.ApacheDsLdap;
import org.infinispan.test.integration.security.utils.Deployments;
import org.infinispan.test.integration.security.utils.Utils;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.arquillian.api.ServerSetup;
import org.jboss.as.arquillian.api.ServerSetupTask;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.test.integration.security.common.config.SecurityDomain;
import org.jboss.as.test.integration.security.common.config.SecurityModule;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:vjuranek@redhat.com">Vojtech Juranek</a>
 * @since 7.0
 */
@RunWith(Arquillian.class)
@ServerSetup({
      LdapAuthenticationIT.SecurityDomainsSetupTask.class,
      LdapAuthenticationIT.SecurityTraceLoggingServerSetupTask.class,
      LdapAuthenticationIT.LdapServerSetupTask.class,
})
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

   @Deployment
   @TargetsContainer(DEFAULT_DEPLOY_CONTAINER)
   public static WebArchive getDeployment() {
      return Deployments.createKrbLdapTestDeployment();
   }

   public Map<String, AuthorizationPermission[]> getRolePermissionMap() {
      Map<String, AuthorizationPermission[]> roles = new HashMap<String, AuthorizationPermission[]>();
      roles.put(ADMIN_ROLE, new AuthorizationPermission[]{AuthorizationPermission.ALL});
      roles.put(WRITER_ROLE, new AuthorizationPermission[]{AuthorizationPermission.WRITE});
      roles.put(READER_ROLE, new AuthorizationPermission[]{AuthorizationPermission.READ});
      roles.put(UNPRIVILEGED_ROLE, new AuthorizationPermission[]{AuthorizationPermission.NONE});
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

   /**
    * A Trace logging server setup task. Sets trace logging for specified packages
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class SecurityTraceLoggingServerSetupTask extends AbstractTraceLoggingServerSetupTask {

      @Override
      protected Collection<String> getCategories(ManagementClient managementClient, String containerId) {
         return Arrays.asList("javax.security", "org.jboss.security", "org.picketbox", "org.wildfly.security");
      }
   }

   /**
    * A Kerberos/Ldap server setup task. Starts Ldap server
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class LdapServerSetupTask implements ServerSetupTask {
      private static ApacheDsLdap ldapServer;

      @Override
      public void setup(ManagementClient managementClient, String s) throws Exception {
         ldapServer = new ApacheDsLdap();
         ldapServer.start();
      }

      @Override
      public void tearDown(ManagementClient managementClient, String s) throws Exception {
         ldapServer.stop();
      }
   }

   /**
    * A {@link ServerSetupTask} instance which creates security domains for this test case.
    *
    * @author jcacek@redhat,com
    * @author vchepeli@redhat,com
    */
   static class SecurityDomainsSetupTask extends AbstractSecurityDomainsServerSetupTask {

      /**
       * Returns SecurityDomains configuration for this testcase.
       */
      @Override
      protected SecurityDomain[] getSecurityDomains() {
         final String hostname = Utils.getCannonicalHost(managementClient);
         final String ldapUrl = "ldap://" + hostname + ":" + "10389";
         final SecurityDomain sd = new SecurityDomain.Builder()
               .name(SECURITY_DOMAIN_NAME)
               .cacheType("default")
               .loginModules(
                     new SecurityModule.Builder()
                           .name("org.jboss.security.auth.spi.LdapLoginModule")
                           .flag("required")

                           .putOption(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                           .putOption("java.naming.provider.url", ldapUrl)
                           .putOption(Context.SECURITY_AUTHENTICATION, "simple")

                           .putOption("principalDNPrefix", "uid=")
                           .putOption("principalDNSuffix", ",ou=People,dc=infinispan,dc=org")

                           .putOption("rolesCtxDN", "ou=Roles,dc=infinispan,dc=org")
                           .putOption("uidAttributeID", "member")
                           .putOption("matchOnUserDN", "true")
                           .putOption("roleAttributeID", "cn")
                           .putOption("roleAttributeIsDN", "false")
                           .putOption("searchScope", "ONELEVEL_SCOPE")
                           .build())
               .build();
         return new SecurityDomain[]{sd};
      }
   }
}
