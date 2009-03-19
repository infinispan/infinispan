package org.horizon.jmx;

import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.GlobalComponentRegistry;
import org.horizon.manager.CacheManager;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.MBeanInfo;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import java.lang.management.ManagementFactory;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.GlobalComponentsRegitrationTest")
public class GlobalComponentsRegitrationTest {

   private MBeanServer mBeanServer;
   private CacheManager cacheManager;
   private PlatformMBeanServerGlobalRegistration registration;
   private static final String JMX_NAME_BASE = "horizion-test:";

   @BeforeMethod
   public void setUp() {
//      mBeanServer = MBeanServerFactory.createMBeanServer();
      mBeanServer = ManagementFactory.getPlatformMBeanServer();
      GlobalConfiguration globalConfiguration = TestingUtil.getGlobalConfiguration();
      globalConfiguration.setExposeGlobalManagementStatistics(false);
      cacheManager = TestingUtil.createClusteredCacheManager(globalConfiguration);
      cacheManager.start();

      Configuration config = new Configuration();
      config.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      config.setExposeManagementStatistics(false);
      cacheManager.defineCache("a", config);
      cacheManager.getCache("a");
      registration = new PlatformMBeanServerGlobalRegistration();
      registration.setMBeanServer(mBeanServer);

      GlobalComponentRegistry componentRegistry = TestingUtil.extractGlobalComponentRegistry(cacheManager);
      GlobalConfiguration configuration = GlobalConfiguration.getClusteredDefault();
      configuration.setJmxDomain(JMX_NAME_BASE);
      registration.init(componentRegistry, configuration);
      registration.start();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
      MBeanServerFactory.releaseMBeanServer(mBeanServer);
      registration.stop();
   }

   public void testRpcManagerAttributes() throws Exception {
      registration = new PlatformMBeanServerGlobalRegistration();
      ObjectName rpcManagerObjectName = getObjectName("RPCManager");
      assert mBeanServer.isRegistered(rpcManagerObjectName);
      MBeanInfo beanInfo = mBeanServer.getMBeanInfo(rpcManagerObjectName);
      assert attributeExists(beanInfo, "ReplicationCount");
      assert attributeExists(beanInfo, "ReplicationFailures");
      assert attributeExists(beanInfo, "StatisticsEnabled");
      assert attributeExists(beanInfo, "SuccessRatio");

      assert operationExists(beanInfo, "resetStatistics");
   }

   private boolean operationExists(MBeanInfo beanInfo, String opName) {
      MBeanOperationInfo[] beanOperationInfos = beanInfo.getOperations();
      for (MBeanOperationInfo attributeInfo: beanOperationInfos) {
         if (attributeInfo.getName().equals(opName)) {
            return true;
         }
      }
      return false;

   }

   private boolean attributeExists(MBeanInfo beanInfo, String attrName) {
      for (MBeanAttributeInfo attributeInfo: beanInfo.getAttributes()) {
         if (attributeInfo.getName().equals(attrName)) {
            return true;
         }
      }
      return false;
   }

   //todo mmarkus enable test
   @Test(enabled = false)
   public void testCacheMgmtInterceptor() throws Exception {
      ObjectName interceptor = getObjectName("CacheMgmtInterceptor");
//      sleepForever();
      assert mBeanServer.isRegistered(interceptor);
   }

   private void sleepForever() {
      while (true) {
         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }
   }

   private ObjectName getObjectName(String name) {
      try {
         return new ObjectName(ComponentGroupJmxRegistration.getObjectName(JMX_NAME_BASE,PlatformMBeanServerGlobalRegistration.GLOBAL_JMX_GROUP, name));
      } catch (MalformedObjectNameException e) {
         throw new IllegalStateException(e);
      }
   }
}
