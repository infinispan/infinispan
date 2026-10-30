package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Tests the {@code clusterMemberVersions} and {@code rollingUpgrade} attributes of the CacheManager MBean.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "jmx.ClusterMemberVersionsMBeanTest")
public class ClusterMemberVersionsMBeanTest extends MultipleCacheManagersTest {

   private static final String JMX_DOMAIN = ClusterMemberVersionsMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Throwable {
      addNode();
      addNode();
      blockUntilViewsReceived();
   }

   public void testEveryMemberReportsTheVersionOfTheWholeCluster() {
      Map<String, String> expected = expectedVersions(address(0).toString(), address(1).toString());
      eventuallyEquals(expected, () -> memberVersions(0));
      eventuallyEquals(expected, () -> memberVersions(1));
   }

   public void testHomogeneousClusterIsNotARollingUpgrade() {
      // make sure the versions have been collected before asserting on them
      eventuallyEquals(expectedVersions(address(0).toString(), address(1).toString()), () -> memberVersions(0));
      assertFalse(rollingUpgrade(0));
      assertFalse(rollingUpgrade(1));
   }

   public void testMemberVersionsFollowTheClusterView() {
      String address0 = address(0).toString();
      String address1 = address(1).toString();

      addNode();
      blockUntilViewsReceived();
      String address2 = address(2).toString();

      Map<String, String> threeMembers = expectedVersions(address0, address1, address2);
      eventuallyEquals(threeMembers, () -> memberVersions(0));
      eventuallyEquals(threeMembers, () -> memberVersions(2));

      TestingUtil.killCacheManagers(manager(2));
      cacheManagers.remove(2);
      blockUntilViewsReceived();

      Map<String, String> twoMembers = expectedVersions(address0, address1);
      eventuallyEquals(twoMembers, () -> memberVersions(0));
      eventuallyEquals(twoMembers, () -> memberVersions(1));
   }

   private EmbeddedCacheManager addNode() {
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      configureJmx(global, jmxDomain(cacheManagers.size()), mBeanServerLookup);
      return addClusterEnabledCacheManager(global, null, new TransportFlags());
   }

   private void blockUntilViewsReceived() {
      // the view may still contain a killed member, so do not barf if there are too many members
      TestingUtil.blockUntilViewsReceived(30000, false, cacheManagers.toArray(new EmbeddedCacheManager[0]));
   }

   private Map<String, String> expectedVersions(String... addresses) {
      Map<String, String> expected = new HashMap<>();
      for (String address : addresses) {
         expected.put(address, Version.getVersion());
      }
      return expected;
   }

   /**
    * Reads the flat address/version pair array exposed by JMX back into a map.
    */
   private Map<String, String> memberVersions(int index) {
      String[] addressesAndVersions = (String[]) attribute(index, "clusterMemberVersions");
      assertEquals(0, addressesAndVersions.length % 2);
      Map<String, String> versions = new HashMap<>();
      for (int i = 0; i < addressesAndVersions.length; i += 2) {
         versions.put(addressesAndVersions[i], addressesAndVersions[i + 1]);
      }
      return versions;
   }

   private boolean rollingUpgrade(int index) {
      return (Boolean) attribute(index, "rollingUpgrade");
   }

   private Object attribute(int index, String attributeName) {
      try {
         MBeanServer server = mBeanServerLookup.getMBeanServer();
         return server.getAttribute(getCacheManagerObjectName(jmxDomain(index)), attributeName);
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   private String jmxDomain(int index) {
      return JMX_DOMAIN + index;
   }
}
