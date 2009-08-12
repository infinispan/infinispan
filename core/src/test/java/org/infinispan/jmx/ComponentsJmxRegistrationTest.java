package org.infinispan.jmx;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tester class for {@link ComponentsJmxRegistration}.
 *
 * @author Mircea.Markus@jboss.com
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.ComponentsJmxRegistrationTest")
public class ComponentsJmxRegistrationTest {

   private MBeanServer mBeanServer;
   private List<CacheManager> cacheManagers = new ArrayList<CacheManager>();

   @BeforeMethod
   public void setUp() {
      mBeanServer = MBeanServerFactory.createMBeanServer();
      cacheManagers.clear();
   }

   @AfterMethod
   public void tearDown() {
      MBeanServerFactory.releaseMBeanServer(mBeanServer);
      for (CacheManager cacheManager : cacheManagers) {
         TestingUtil.killCacheManagers(cacheManager);
      }
      cacheManagers.clear();
   }

   public void testRegisterLocalCache() throws Exception {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      cacheManagers.add(cm);
      cm.start();
      Configuration configuration = config();
      configuration.setCacheMode(Configuration.CacheMode.LOCAL);
      cm.defineConfiguration("first", configuration);
      Cache first = cm.getCache("first");

      ComponentsJmxRegistration regComponents = buildRegistrator(first);
      regComponents.registerMBeans();
      String name = regComponents.getObjectName("Statistics");
      ObjectName name1 = new ObjectName(name);
      assert mBeanServer.isRegistered(name1);
      regComponents.unregisterMBeans();
      assert !mBeanServer.isRegistered(name1);
      assertCorrectJmxName(name1, first);
   }

   private ComponentsJmxRegistration buildRegistrator(Cache cache) {
      AdvancedCache ac = (AdvancedCache) cache;
      Set<AbstractComponentRegistry.Component> components = ac.getComponentRegistry().getRegisteredComponents();
      return new ComponentsJmxRegistration(mBeanServer, components, cache.getName());
   }

   public void testRegisterReplicatedCache() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setAllowDuplicateDomains(true);
      CacheManager cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      cacheManagers.add(cm);
      cm.start();
      Configuration configurationOverride = config();
      configurationOverride.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration("first", configurationOverride);
      Cache first = cm.getCache("first");

      ComponentsJmxRegistration regComponents = buildRegistrator(first);
      regComponents.registerMBeans();
      String name = regComponents.getObjectName("Statistics");
      ObjectName name1 = new ObjectName(name);
      assertCorrectJmxName(name1, first);
      assert mBeanServer.isRegistered(name1);
      regComponents.unregisterMBeans();
      assert !mBeanServer.isRegistered(name1);
   }

   public void testLocalAndReplicatedCache() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setAllowDuplicateDomains(true);
      CacheManager cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      cacheManagers.add(cm);
      cm.start();
      Configuration replicated = config();
      Configuration local = config();
      replicated.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      local.setCacheMode(Configuration.CacheMode.LOCAL);
      cm.defineConfiguration("replicated", replicated);
      cm.defineConfiguration("local", local);
      Cache replicatedCache = cm.getCache("replicated");
      Cache localCache = cm.getCache("local");

      ComponentsJmxRegistration replicatedRegComponents = buildRegistrator(replicatedCache);
      ComponentsJmxRegistration localRegComponents = buildRegistrator(localCache);
      replicatedRegComponents.registerMBeans();
      localRegComponents.registerMBeans();

      String replicatedtCMgmtIntName = replicatedRegComponents.getObjectName("Statistics");
      String localCMgmtIntName = localRegComponents.getObjectName("Statistics");
      ObjectName replObjectName = new ObjectName(replicatedtCMgmtIntName);
      ObjectName localObjName = new ObjectName(localCMgmtIntName);
      assertCorrectJmxName(replObjectName, replicatedCache);

      assert mBeanServer.isRegistered(replObjectName);
      assert mBeanServer.isRegistered(localObjName);
      assert !localCMgmtIntName.equals(replicatedtCMgmtIntName);

      replicatedRegComponents.unregisterMBeans();
      localRegComponents.unregisterMBeans();
      assert !mBeanServer.isRegistered(new ObjectName(localCMgmtIntName));
      assert !mBeanServer.isRegistered(new ObjectName(replicatedtCMgmtIntName));
   }

   private void assertCorrectJmxName(ObjectName objectName, Cache cache) {
      assert objectName.getKeyProperty(ComponentsJmxRegistration.CACHE_NAME_KEY).startsWith(cache.getName());
      assert objectName.getKeyProperty(ComponentsJmxRegistration.JMX_RESOURCE_KEY) != null;
   }

   private Configuration config() {
      Configuration configuration = new Configuration();
      configuration.setFetchInMemoryState(false);
      configuration.setExposeJmxStatistics(true);
      return configuration;
   }
}
