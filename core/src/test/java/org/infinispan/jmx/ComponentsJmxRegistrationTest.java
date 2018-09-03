package org.infinispan.jmx;

import static java.util.Collections.singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link ComponentsJmxRegistration}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.ComponentsJmxRegistrationTest")
public class ComponentsJmxRegistrationTest extends AbstractInfinispanTest {
   public static final String JMX_DOMAIN = ComponentsJmxRegistrationTest.class.getSimpleName();
   private MBeanServer mBeanServer;
   private List<EmbeddedCacheManager> cacheContainers = new ArrayList<>();

   @BeforeMethod
   public void setUp() {
      mBeanServer = MBeanServerFactory.createMBeanServer();
      cacheContainers.clear();
   }

   @AfterMethod
   public void tearDown() {
      MBeanServerFactory.releaseMBeanServer(mBeanServer);
      TestingUtil.killCacheManagers(cacheContainers);
      cacheContainers.clear();
   }

   public void testStopStartCM() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cacheContainers.add(cm);
      cm.stop();
      cm.start();
   }

   public void testRegisterLocalCache() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cacheContainers.add(cm);
      cm.start();
      ConfigurationBuilder configuration = config();
      configuration.clustering().cacheMode(CacheMode.LOCAL);
      cm.defineConfiguration("first", configuration.build());
      Cache first = cm.getCache("first");

      ComponentMetadataRepo metadataRepo = cm.getGlobalComponentRegistry().getComponentMetadataRepo();
      ComponentMetadata metadata = metadataRepo.getComponentMetadata(first.getClass());
      ResourceDMBean mbean = new ResourceDMBean(first, metadata.toManageableComponentMetadata());
      Collection<ResourceDMBean> mbeans = singleton(mbean);

      ComponentsJmxRegistration regComponents = buildRegistrator(first);
      regComponents.registerMBeans(mbeans);
      String name = regComponents.getObjectName("Cache").toString();
      ObjectName name1 = new ObjectName(name);
      assert mBeanServer.isRegistered(name1);
      regComponents.unregisterMBeans(mbeans);
      assert !mBeanServer.isRegistered(name1);
      assertCorrectJmxName(name1, first);
   }

   private ComponentsJmxRegistration buildRegistrator(Cache cache) {
      String groupName = "name=" + ObjectName.quote(cache.getName());
      ComponentsJmxRegistration registrator = new ComponentsJmxRegistration(mBeanServer, groupName);
      registrator.setJmxDomain(JMX_DOMAIN);
      return registrator;
   }

   public void testRegisterReplicatedCache() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      cacheContainers.add(cm);
      cm.start();
      ConfigurationBuilder configurationOverride = config();
      configurationOverride.clustering().cacheMode(CacheMode.REPL_SYNC);
      cm.defineConfiguration("first", configurationOverride.build());
      Cache first = cm.getCache("first");

      ComponentMetadataRepo metadataRepo = cm.getGlobalComponentRegistry().getComponentMetadataRepo();
      ComponentMetadata metadata = metadataRepo.getComponentMetadata(first.getClass());
      ResourceDMBean mbean = new ResourceDMBean(first, metadata.toManageableComponentMetadata());
      Collection<ResourceDMBean> mbeans = singleton(mbean);

      ComponentsJmxRegistration regComponents = buildRegistrator(first);
      regComponents.registerMBeans(mbeans);
      String name = regComponents.getObjectName("Cache").toString();
      ObjectName name1 = new ObjectName(name);
      assertCorrectJmxName(name1, first);
      assert mBeanServer.isRegistered(name1);
      regComponents.unregisterMBeans(mbeans);
      assert !mBeanServer.isRegistered(name1);
   }

   public void testLocalAndReplicatedCache() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfiguration.globalJmxStatistics().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfiguration, new ConfigurationBuilder());
      cacheContainers.add(cm);
      cm.start();
      ConfigurationBuilder replicated = config();
      ConfigurationBuilder local = config();
      replicated.clustering().cacheMode(CacheMode.REPL_SYNC);
      local.clustering().cacheMode(CacheMode.LOCAL);
      cm.defineConfiguration("replicated", replicated.build());
      cm.defineConfiguration("local", local.build());
      Cache replicatedCache = cm.getCache("replicated");
      Cache localCache = cm.getCache("local");

      ComponentMetadataRepo metadataRepo = cm.getGlobalComponentRegistry().getComponentMetadataRepo();
      ComponentMetadata metadata = metadataRepo.getComponentMetadata(replicatedCache.getClass());
      ResourceDMBean replicatedMBean = new ResourceDMBean(replicatedCache, metadata.toManageableComponentMetadata());
      ResourceDMBean localMBean = new ResourceDMBean(localCache, metadata.toManageableComponentMetadata());

      ComponentsJmxRegistration replicatedRegComponents = buildRegistrator(replicatedCache);
      ComponentsJmxRegistration localRegComponents = buildRegistrator(localCache);
      replicatedRegComponents.registerMBeans(singleton(replicatedMBean));
      localRegComponents.registerMBeans(singleton(localMBean));

      String replicatedtCMgmtIntName = replicatedRegComponents.getObjectName("Cache").toString();
      String localCMgmtIntName = localRegComponents.getObjectName("Cache").toString();
      ObjectName replObjectName = new ObjectName(replicatedtCMgmtIntName);
      ObjectName localObjName = new ObjectName(localCMgmtIntName);
      assertCorrectJmxName(replObjectName, replicatedCache);

      assert mBeanServer.isRegistered(replObjectName);
      assert mBeanServer.isRegistered(localObjName);
      assert !localCMgmtIntName.equals(replicatedtCMgmtIntName);

      replicatedRegComponents.unregisterMBeans(singleton(replicatedMBean));
      localRegComponents.unregisterMBeans(singleton(localMBean));
      assert !mBeanServer.isRegistered(new ObjectName(localCMgmtIntName));
      assert !mBeanServer.isRegistered(new ObjectName(replicatedtCMgmtIntName));
   }

   private void assertCorrectJmxName(ObjectName objectName, Cache cache) {
      assert ObjectName.unquote(objectName.getKeyProperty(ComponentsJmxRegistration.NAME_KEY)).startsWith(cache.getName());
      assert objectName.getKeyProperty(ComponentsJmxRegistration.COMPONENT_KEY) != null;
   }

   private ConfigurationBuilder config() {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.clustering().stateTransfer().fetchInMemoryState(false).jmxStatistics().enable();
      return configuration;
   }
}
