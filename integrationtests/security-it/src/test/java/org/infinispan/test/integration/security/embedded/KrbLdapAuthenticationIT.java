package org.infinispan.test.integration.security.embedded;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.test.integration.security.tasks.AbstractKrb5ConfServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractSecurityDomainsServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractSystemPropertiesServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractTraceLoggingServerSetupTask;
import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.infinispan.test.integration.security.utils.Deployments;
import org.infinispan.test.integration.security.utils.SimplePrincipalGroupRoleMapper;
import org.infinispan.test.integration.security.utils.Utils;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.arquillian.api.ServerSetup;
import org.jboss.as.arquillian.api.ServerSetupTask;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.test.integration.security.common.config.SecurityDomain;
import org.jboss.as.test.integration.security.common.config.SecurityModule;
import org.jboss.security.SecurityConstants;
import org.jboss.security.negotiation.AdvancedLdapLoginModule;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

/**
 * @author <a href="mailto:vjuranek@redhat.com">Vojtech Juranek</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @since 7.0
 */
@RunWith(Arquillian.class)
@ServerSetup({
      KrbLdapAuthenticationIT.KerberosSystemPropertiesSetupTask.class,
      KrbLdapAuthenticationIT.SecurityDomainsSetupTask.class,
      KrbLdapAuthenticationIT.SecurityTraceLoggingServerSetupTask.class,
      KrbLdapAuthenticationIT.KrbLdapServerSetupTask.class,
      KrbLdapAuthenticationIT.Krb5ConfServerSetupTask.class
})
public class KrbLdapAuthenticationIT extends AbstractAuthentication {

   private static final String TRUE = Boolean.TRUE.toString(); // TRUE

   public static final String ADMIN_ROLE = "AdminIspnRole";
   public static final String WRITER_ROLE = "WriterIspnRole";
   public static final String READER_ROLE = "ReaderIspnRole";
   public static final String UNPRIVILEGED_ROLE = "UnprivilegedIspnRole";

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

   /**
    * A {@link ServerSetupTask} instance which creates security domains for this test case.
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class SecurityDomainsSetupTask extends AbstractSecurityDomainsServerSetupTask {
      /**
       * Returns SecurityDomains configuration for this testcase.
       */
      @Override
      protected SecurityDomain[] getSecurityDomains() {
         final String host = Utils.getCannonicalHost(managementClient);
         final SecurityDomain krbLdapServiceDomain = getKrbSecurityDomain("ldap-service", "ldap/" + host, true);
         final SecurityDomain krbAdminDomain = getKrbSecurityDomain("admin", "admin", false);
         final SecurityDomain krbWriterDomain = getKrbSecurityDomain("writer", "writer", false);
         final SecurityDomain krbReaderDomain = getKrbSecurityDomain("reader", "reader", false);
         final SecurityDomain krbUnprivDomain = getKrbSecurityDomain("unprivileged", "unprivileged", false);
         final SecurityDomain spnegoAdminDomain = getSpnegoSecurityDomain("admin", managementClient, 10389, "ldap-service");
         final SecurityDomain spnegoWriterDomain = getSpnegoSecurityDomain("writer", managementClient, 10389, "ldap-service");
         final SecurityDomain spnegoReaderDomain = getSpnegoSecurityDomain("reader", managementClient, 10389, "ldap-service");
         final SecurityDomain spnegoUnprivDomain = getSpnegoSecurityDomain("unprivileged", managementClient, 10389, "ldap-service");
         return new SecurityDomain[]{
               krbLdapServiceDomain,
               krbAdminDomain, krbWriterDomain, krbReaderDomain, krbUnprivDomain,
               spnegoAdminDomain, spnegoWriterDomain, spnegoReaderDomain, spnegoUnprivDomain
         };
      }

      private SecurityDomain getKrbSecurityDomain(String name, String principal, boolean isService) {
         final SecurityModule.Builder krbModuleBuilder = new SecurityModule.Builder();
         if (Utils.IBM_JDK) {
            krbModuleBuilder.name("com.ibm.security.auth.module.Krb5LoginModule") //
                  .putOption("useKeytab", "${java.io.tmpdir}" + File.separator + "keytabs" + File.separator + name + ".keytab"); //
            if (isService) {
               krbModuleBuilder.putOption("credsType", "both") //
                     .putOption("forwardable", TRUE) //
                     .putOption("proxiable", TRUE) //
                     .putOption("noAddress", TRUE);
            } else
               krbModuleBuilder.putOption("credsType", "acceptor");
         } else {
            krbModuleBuilder.name("Kerberos")
                  .putOption("useKeyTab", TRUE)
                  .putOption("keyTab", "${java.io.tmpdir}" + File.separator + "keytabs" + File.separator + name + ".keytab"); //
            if (isService) {
               krbModuleBuilder
                     .putOption("storeKey", TRUE)
                     .putOption("refreshKrb5Config", TRUE)
                     .putOption("doNotPrompt", TRUE);
            }
         }
         krbModuleBuilder
               .putOption("principal", principal + "@INFINISPAN.ORG")
               .putOption("debug", TRUE);

         return new SecurityDomain.Builder().name("krb-" + name)
               .cacheType("default")
               .loginModules(krbModuleBuilder.build())
               .build();
      }

      private static SecurityDomain getSpnegoSecurityDomain(String user, ManagementClient managementClient, int ldapPort, String ldapServiceName) {
         final String host = Utils.getCannonicalHost(managementClient);
         return new SecurityDomain.Builder()
               .name("ispn-" + user)
               .cacheType("default")
               .loginModules(
                     new SecurityModule.Builder().name("SPNEGO").flag("requisite")
                           .putOption("password-stacking", "useFirstPass")
                           .putOption("serverSecurityDomain", "krb-" + ldapServiceName)
                           .putOption("usernamePasswordDomain", "krb-" + user)
                           .build(),
                     new SecurityModule.Builder().name(AdvancedLdapLoginModule.class.getName())
                           .putOption("password-stacking", "useFirstPass")

//                           .putOption("bindAuthentication", "simple")
//                           .putOption("bindCredential", "secret")
//                           .putOption("bindDN", "uid=admin,ou=system")

                           .putOption("bindAuthentication", "GSSAPI")
                           .putOption("jaasSecurityDomain", "krb-" + ldapServiceName)

                           .putOption(Context.PROVIDER_URL,
                                      "ldap://" + host + ":" + ldapPort)
                           .putOption("baseCtxDN", "ou=People,dc=infinispan,dc=org")
                           .putOption("baseFilter", "(krb5PrincipalName={0})")
                           .putOption("rolesCtxDN", "ou=Roles,dc=infinispan,dc=org")
                           .putOption("roleFilter", "(member={1})")
                           .putOption("roleAttributeID", "cn")
                           .build())
               .build();
      }
   }

   /**
    * A Kerberos system-properties server setup task. Sets path to a <code>krb5.conf</code> file and enables Kerberos
    * debug messages.
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class KerberosSystemPropertiesSetupTask extends AbstractSystemPropertiesServerSetupTask {
      /**
       * Returns "java.security.krb5.conf" and "sun.security.krb5.debug" properties.
       *
       * @return Kerberos properties
       */
      @Override
      protected SystemProperty[] getSystemProperties() {
         final Map<String, String> map = new HashMap<>();
         map.put("java.security.krb5.conf", "${java.io.tmpdir}" + File.separator + "krb5.conf");
         map.put("java.security.krb5.debug", TRUE);
         map.put(SecurityConstants.DISABLE_SECDOMAIN_OPTION, TRUE);
         return mapToSystemProperties(map);
      }
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
    * A Kerberos/Ldap server setup task. Starts Kerberos/Ldap server
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class KrbLdapServerSetupTask implements ServerSetupTask {
      private static ApacheDsKrbLdap krbLdapServer;

      @Override
      public void setup(ManagementClient managementClient, String s) throws Exception {
         final String hostname = Utils.getCannonicalHost(managementClient);
         System.setProperty("java.security.krb5.conf", Utils.getResource("krb5.conf").getPath());
         krbLdapServer = new ApacheDsKrbLdap(hostname);
         krbLdapServer.start();
      }

      @Override
      public void tearDown(ManagementClient managementClient, String s) throws Exception {
         krbLdapServer.stop();
      }
   }

   /**
    * Generate kerberos keytabs for infinispan users and ldap service
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class Krb5ConfServerSetupTask extends AbstractKrb5ConfServerSetupTask {

      public static final File ADMIN_KEYTAB_FILE = new File(KEYTABS_DIR, "admin.keytab");
      public static final File WRITER_KEYTAB_FILE = new File(KEYTABS_DIR, "writer.keytab");
      public static final File READER_KEYTAB_FILE = new File(KEYTABS_DIR, "reader.keytab");
      public static final File UNPRIV_KEYTAB_FILE = new File(KEYTABS_DIR, "unprivileged.keytab");

      @Override
      protected List<UserForKeyTab> kerberosUsers() {
         List<UserForKeyTab> users = new ArrayList<>();
         users.add(new UserForKeyTab("admin@INFINISPAN.ORG", "strongPassword", ADMIN_KEYTAB_FILE));
         users.add(new UserForKeyTab("writer@INFINISPAN.ORG", "somePassword", WRITER_KEYTAB_FILE));
         users.add(new UserForKeyTab("reader@INFINISPAN.ORG", "password", READER_KEYTAB_FILE));
         users.add(new UserForKeyTab("unprivileged@INFINISPAN.ORG", "weakPassword", UNPRIV_KEYTAB_FILE));
         return users;
      }
   }
}
