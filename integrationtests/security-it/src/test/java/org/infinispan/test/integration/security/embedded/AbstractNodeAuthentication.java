package org.infinispan.test.integration.security.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.integration.security.tasks.AbstractKrb5ConfServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractSecurityDomainsServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractSystemPropertiesServerSetupTask;
import org.infinispan.test.integration.security.tasks.AbstractTraceLoggingServerSetupTask;
import org.infinispan.test.integration.security.utils.ApacheDsKrbLdap;
import org.infinispan.test.integration.security.utils.ManagementClientParams;
import org.infinispan.test.integration.security.utils.Utils;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.as.arquillian.api.ServerSetupTask;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.test.integration.security.common.config.SecurityDomain;
import org.jboss.as.test.integration.security.common.config.SecurityModule;
import org.jboss.security.SecurityConstants;
import org.junit.Test;
import org.wildfly.test.api.Authentication;

/**
 * @author <a href="mailto:vjuranek@redhat.com">Vojtech Juranek</a>
 * @since 7.0
 */
public abstract class AbstractNodeAuthentication {

   protected static final String COORDINATOR_NODE = "node0";
   protected static final String COORDINATOR_JGROUSP_CONFIG_MD5 = "jgroups-tcp-sasl-md5-node0.xml";
   protected static final String COORDINATOR_JGROUSP_CONFIG_MD5_USER = "jgroups-tcp-sasl-md5-user-node0.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_MD5 = "jgroups-tcp-sasl-md5-node1.xml";
   protected static final String COORDINATOR_JGROUSP_CONFIG_KRB = "jgroups-tcp-sasl-krb-node0.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_KRB = "jgroups-tcp-sasl-krb-node1.xml";
   protected static final String JOINING_NODE_JGROUSP_CONFIG_KRB_FAIL = "jgroups-tcp-sasl-krb-node1-fail.xml";

   protected static final String CACHE_NAME = "replicatedCache";
   protected static final String TEST_ITEM_KEY = "test_key";
   protected static final String TEST_ITEM_VALUE = "test_value";

   private static final String TRUE = Boolean.TRUE.toString(); // TRUE
   private static final Log LOG = LogFactory.getLog(AbstractNodeAuthentication.class);
   private final boolean krbProvided;

   @ArquillianResource
   protected ContainerController controller;

   @ArquillianResource
   protected Deployer deployer;

   protected abstract String getCoordinatorNodeConfig();

   protected abstract String getJoiningNodeName();

   protected abstract String getJoiningNodeConfig();

   public AbstractNodeAuthentication(boolean krbProvided) {
      this.krbProvided = krbProvided;
   }

   protected Cache<String, String> getReplicatedCache(EmbeddedCacheManager manager) throws Exception {
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheConfig.invocationBatching().enable();
      cacheConfig.jmxStatistics().disable();
      cacheConfig.clustering().cacheMode(CacheMode.REPL_SYNC);

      manager.defineConfiguration(CACHE_NAME, cacheConfig.build());
      Cache<String, String> replicatedCache = manager.getCache(CACHE_NAME);

      return replicatedCache;
   }

   protected EmbeddedCacheManager getCacheManager(String jgrousConfigFile) {
      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder();
      globalConfig.globalJmxStatistics().disable();
      globalConfig.globalJmxStatistics().mBeanServerLookup(null); //TODO remove once WFLY-3124 is fixed, for now fail JMX registration
      globalConfig.transport().defaultTransport().addProperty("configurationFile", jgrousConfigFile);
      EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig.build());
      return manager;
   }

   @Test
   @InSequence(1)
   public void startNodes() throws Exception {
      controller.start(COORDINATOR_NODE);
      assertTrue(controller.isStarted(COORDINATOR_NODE));
      controller.start(getJoiningNodeName());
      assertTrue(controller.isStarted(getJoiningNodeName()));

      if (krbProvided) {
         for (ManagementClientParams params : getManagementClientListParams()) {
            ModelControllerClient client = getModelControllerClient(params);
            ManagementClient managementClient = new ManagementClient(client, params.getHostname(),
                                                                     params.getPort(), "http-remoting");
            KerberosSystemPropertiesSetupTask.INSTANCE.setup(managementClient, null);
            SecurityTraceLoggingServerSetupTask.INSTANCE.setup(managementClient, null);
            SecurityDomainsSetupTask.INSTANCE.setup(managementClient, null);
         }
      }

      deployer.deploy(COORDINATOR_NODE);
      deployer.deploy(getJoiningNodeName());
   }

   @Test
   @OperateOnDeployment(COORDINATOR_NODE)
   @InSequence(2)
   public void testCreateItemOnCoordinator() throws Exception {
      Cache<String, String> cache = getReplicatedCache(getCacheManager(getCoordinatorNodeConfig()));
      cache.put(TEST_ITEM_KEY, TEST_ITEM_VALUE);
      assertEquals(TEST_ITEM_VALUE, cache.get(TEST_ITEM_KEY));
   }

   @Test
   @InSequence(3)
   //Needs to be overwritten in test class, which should add annotation @OperateOnDeployment(getJoiningNodeName())
   public void testReadItemOnJoiningNode() throws Exception {
      EmbeddedCacheManager manager = getCacheManager(getJoiningNodeConfig());
      Cache<String, String> cache = getReplicatedCache(manager);
      assertEquals("Insufficient number of cluster members", 2, manager.getMembers().size());
      assertEquals(TEST_ITEM_VALUE, cache.get(TEST_ITEM_KEY));
   }

   @Test
   @InSequence(4)
   public void stopJoiningNodes() throws Exception {
      deployer.undeploy(getJoiningNodeName());
      deployer.undeploy(COORDINATOR_NODE);

      if (krbProvided) {
         for (ManagementClientParams params : getManagementClientListParams()) {
            ModelControllerClient client = getModelControllerClient(params);
            ManagementClient managementClient = new ManagementClient(client, params.getHostname(),
                                                                     params.getPort(), "http-remoting");

            KerberosSystemPropertiesSetupTask.INSTANCE.tearDown(managementClient, null);
            SecurityTraceLoggingServerSetupTask.INSTANCE.tearDown(managementClient, null);
            SecurityDomainsSetupTask.INSTANCE.tearDown(managementClient, null);
         }
      }

      try {
         controller.stop(getJoiningNodeName());
      } catch (Exception e) {
         LOG.warn("Joining node stop failed with %s", e.getCause());
         controller.kill(getJoiningNodeName());
      }
      try {
         controller.stop(COORDINATOR_NODE);
      } catch (Exception e) {
         LOG.warn("Coordinator node stop failed with %s", e.getCause());
         controller.kill(COORDINATOR_NODE);
      }
      assertFalse(controller.isStarted(getJoiningNodeName()));
      assertFalse(controller.isStarted(COORDINATOR_NODE));
   }

   public static ModelControllerClient getModelControllerClient(ManagementClientParams params) {
      try {
         return ModelControllerClient.Factory.create(
               InetAddress.getByName(params.getHostname()),
               params.getPort(),
               Authentication.getCallbackHandler()
         );
      } catch (UnknownHostException e) {
         throw new RuntimeException(e);
      }
   }

   List<ManagementClientParams> getManagementClientListParams() {
      List<ManagementClientParams> list = new ArrayList<>();

      for (int i = 0; i < 2; i++) {
         ManagementClientParams params = new ManagementClientParams(
               System.getProperty("node" + i + ".mgmt.addr"), 10090 + 100 * i
         );
         list.add(i, params);
      }
      return list;
   }

   /**
    * A Kerberos system-properties server setup task. Sets path to a <code>krb5.conf</code> file and enables Kerberos
    * debug messages.
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class KerberosSystemPropertiesSetupTask extends AbstractSystemPropertiesServerSetupTask {

      public static final KerberosSystemPropertiesSetupTask INSTANCE = new KerberosSystemPropertiesSetupTask();

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

      public static final SecurityTraceLoggingServerSetupTask INSTANCE = new SecurityTraceLoggingServerSetupTask();

      @Override
      protected Collection<String> getCategories(ManagementClient managementClient, String containerId) {
         return Arrays.asList("javax.security", "org.jboss.security", "org.picketbox", "org.wildfly.security");
      }
   }

   /**
    * A {@link ServerSetupTask} instance which creates security domains for this test case.
    *
    * @author jcacek@redhat.com
    * @author vchepeli@redhat.com
    */
   static class SecurityDomainsSetupTask extends AbstractSecurityDomainsServerSetupTask {

      public static final String SECURITY_DOMAIN_PREFIX = "krb-";
      private static final String KEYTABS_DIR = "${java.io.tmpdir}" + File.separator + "keytabs" + File.separator;

      public static final SecurityDomainsSetupTask INSTANCE = new SecurityDomainsSetupTask();

      /**
       * Returns SecurityDomains configuration for this testcase.
       */
      @Override
      protected SecurityDomain[] getSecurityDomains() {

         final SecurityDomain krbNode0 = getKrbSecurityDomain("node0", KEYTABS_DIR + "jgroups_node0_clustered.keytab", "jgroups/node0/clustered@INFINISPAN.ORG");
         final SecurityDomain krbNode1 = getKrbSecurityDomain("node1", KEYTABS_DIR + "jgroups_node1_clustered.keytab", "jgroups/node1/clustered@INFINISPAN.ORG");
         final SecurityDomain krbFail = getKrbSecurityDomain("node1-fail", KEYTABS_DIR + "jgroups_node0_fail_clustered.keytab", "jgroups/node1/clustered2@INFINISPAN.ORG");

         return new SecurityDomain[]{krbNode0, krbNode1, krbFail};
      }

      private SecurityDomain getKrbSecurityDomain(String name, String path, String principal) {
         SecurityModule.Builder smBuilder = new SecurityModule.Builder();
         if (Utils.IBM_JDK) {
            smBuilder.name("com.ibm.security.auth.module.Krb5LoginModule").flag("required")
                  .putOption("useKeytab", path)
                  .putOption("credsType", "both") //
                  .putOption("forwardable", TRUE) //
                  .putOption("proxiable", TRUE) //
                  .putOption("noAddress", TRUE);
         } else {
            smBuilder.name("Kerberos").flag("required") //
                  .putOption("storeKey", "true")
                  .putOption("useKeyTab", "true")
                  .putOption("refreshKrb5Config", "true")
                  .putOption("doNotPrompt", "true")
                  .putOption("keyTab", path); //
         }

         smBuilder
               .putOption("principal", principal + "@INFINISPAN.ORG")
               .putOption("debug", TRUE);

         return new SecurityDomain.Builder()
               .name(SECURITY_DOMAIN_PREFIX + name).cacheType("default")
               .loginModules(
                     smBuilder.build()
               ).build();
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
      private static boolean krbStarted = false;

      @Override
      public void setup(ManagementClient managementClient, String s) throws Exception {
         final String hostname = Utils.getCannonicalHost(managementClient);
         System.setProperty("java.security.krb5.conf", System.getProperty("java.io.tmpdir") + File.separator + "krb5.conf");
         if (!krbStarted) {
            krbLdapServer = new ApacheDsKrbLdap(hostname);
            krbLdapServer.start();
            krbStarted = true;
         }
      }

      @Override
      public void tearDown(ManagementClient managementClient, String s) throws Exception {
         if (krbStarted) {
            krbLdapServer.stop();
            krbStarted = false;
         }
      }
   }

   /**
    * Generate kerberos keytabs for infinispan users and ldap service
    *
    * @author <a href="mailto:jcacek@redhat.com">Josef Cacek</a>
    * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
    */
   static class Krb5ConfServerSetupTask extends AbstractKrb5ConfServerSetupTask {

      public static final File NODE0_KEYTAB_FILE = new File(KEYTABS_DIR, "jgroups_node0_clustered.keytab");
      public static final File NODE1_KEYTAB_FILE = new File(KEYTABS_DIR, "jgroups_node1_clustered.keytab");
      public static final File NODE1_FAIL_KEYTAB_FILE = new File(KEYTABS_DIR, "jgroups_node0_fail_clustered.keytab");
      private static boolean keytabsGenerated = false;

      @Override
      public void setup(ManagementClient managementClient, String containerId) throws Exception {
         if (!keytabsGenerated) {
            super.setup(managementClient, containerId);
            keytabsGenerated = true;
         }
      }

      @Override
      public void tearDown(ManagementClient managementClient, String containerId) throws Exception {
         if (keytabsGenerated) {
            super.tearDown(managementClient, containerId);
            keytabsGenerated = false;
         }
      }

      @Override
      protected List<UserForKeyTab> kerberosUsers() {
         List<UserForKeyTab> users = new ArrayList<>();
         users.add(new UserForKeyTab("jgroups/node0/clustered@INFINISPAN.ORG", "node0password", NODE0_KEYTAB_FILE));
         users.add(new UserForKeyTab("jgroups/node1/clustered@INFINISPAN.ORG", "node1password", NODE1_KEYTAB_FILE));
         users.add(new UserForKeyTab("jgroups/node1/fail/clustered@INFINISPAN.ORG", "failpassword", NODE1_FAIL_KEYTAB_FILE));
         return users;
      }
   }
}
