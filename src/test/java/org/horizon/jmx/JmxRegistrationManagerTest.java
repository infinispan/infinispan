package org.horizon.jmx;

import org.horizon.AdvancedCache;
import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.AbstractComponentRegistry;
import org.horizon.interceptors.CacheMgmtInterceptor;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.test.TestingUtil;
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
 * Tester class for {@link ComponentGroupJmxRegistration}.
 *
 * @author Mircea.Markus@jboss.com
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 1.0
 */
@Test(groups = "functional", testName = "jmx.JmxRegistrationManagerTest")
public class JmxRegistrationManagerTest {

   private MBeanServer mBeanServer;
   private List<CacheManager> cacheManagers = new ArrayList<CacheManager>();

   @BeforeMethod
   public void setUp() {
      mBeanServer = MBeanServerFactory.createMBeanServer();
//      mBeanServer = ManagementFactory.getPlatformMBeanServer();
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
      CacheManager cm = TestingUtil.createLocalCacheManager();
      cacheManagers.add(cm);
      cm.start();
      Configuration configuration = config();
      configuration.setCacheMode(Configuration.CacheMode.LOCAL);
      cm.defineCache("first", configuration);
      Cache first = cm.getCache("first");

      ComponentGroupJmxRegistration regComponentGroup = buildRegistrator(first);
      regComponentGroup.registerMBeans();
      String name = regComponentGroup.getObjectName(CacheMgmtInterceptor.class.getSimpleName());
      ObjectName name1 = new ObjectName(name);
      assert mBeanServer.isRegistered(name1);
      regComponentGroup.unregisterCacheMBeans();
      assert !mBeanServer.isRegistered(name1);
      assertCorrectJmxName(name1, first);
   }

   private ComponentGroupJmxRegistration buildRegistrator(Cache cache) {
      AdvancedCache ac = (AdvancedCache) cache;
      Set<AbstractComponentRegistry.Component> components = ac.getComponentRegistry().getRegisteredComponents();
      return new ComponentGroupJmxRegistration(mBeanServer, components, cache.getName());
   }

   public void testRegisterReplicatedCache() throws Exception {
      GlobalConfiguration globalConfiguration = TestingUtil.getGlobalConfiguration();
      CacheManager cm = new DefaultCacheManager(globalConfiguration);
      cacheManagers.add(cm);
      cm.start();
      Configuration configurationOverride = config();
      configurationOverride.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineCache("first", configurationOverride);
      Cache first = cm.getCache("first");

      ComponentGroupJmxRegistration regComponentGroup = buildRegistrator(first);
      regComponentGroup.registerMBeans();
      String name = regComponentGroup.getObjectName(CacheMgmtInterceptor.class.getSimpleName());
      ObjectName name1 = new ObjectName(name);
      assertCorrectJmxName(name1, first);
      assert mBeanServer.isRegistered(name1);
      regComponentGroup.unregisterCacheMBeans();
      assert !mBeanServer.isRegistered(name1);
   }

   public void testLocalAndReplicatedCache() throws Exception {
      GlobalConfiguration globalConfiguration = TestingUtil.getGlobalConfiguration();
      CacheManager cm = new DefaultCacheManager(globalConfiguration);
      cacheManagers.add(cm);
      cm.start();
      Configuration replicated = config();
      Configuration local = config();
      replicated.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      local.setCacheMode(Configuration.CacheMode.LOCAL);
      cm.defineCache("replicated", replicated);
      cm.defineCache("local", local);
      Cache replicatedCache = cm.getCache("replicated");
      Cache localCache = cm.getCache("local");

      ComponentGroupJmxRegistration replicatedRegComponentGroup = buildRegistrator(replicatedCache);
      ComponentGroupJmxRegistration localRegComponentGroup = buildRegistrator(localCache);
      replicatedRegComponentGroup.registerMBeans();
      localRegComponentGroup.registerMBeans();

      String replicatedtCMgmtIntName = replicatedRegComponentGroup.getObjectName(CacheMgmtInterceptor.class.getSimpleName());
      String localCMgmtIntName = localRegComponentGroup.getObjectName(CacheMgmtInterceptor.class.getSimpleName());
      ObjectName replObjectName = new ObjectName(replicatedtCMgmtIntName);
      ObjectName localObjName = new ObjectName(localCMgmtIntName);
      assertCorrectJmxName(replObjectName, replicatedCache);

      assert mBeanServer.isRegistered(replObjectName);
      assert mBeanServer.isRegistered(localObjName);
      assert !localCMgmtIntName.equals(replicatedtCMgmtIntName);

      replicatedRegComponentGroup.unregisterCacheMBeans();
      localRegComponentGroup.unregisterCacheMBeans();
      assert !mBeanServer.isRegistered(new ObjectName(localCMgmtIntName));
      assert !mBeanServer.isRegistered(new ObjectName(replicatedtCMgmtIntName));
   }

   private void assertCorrectJmxName(ObjectName objectName, Cache cache) {
      assert objectName.getKeyProperty(ComponentGroupJmxRegistration.CACHE_NAME_KEY).startsWith(cache.getName());
      assert objectName.getKeyProperty(ComponentGroupJmxRegistration.JMX_RESOURCE_KEY) != null;
   }

   private Configuration config() {
      Configuration configuration = new Configuration();
      configuration.setExposeManagementStatistics(true);
      return configuration;
   }
}
