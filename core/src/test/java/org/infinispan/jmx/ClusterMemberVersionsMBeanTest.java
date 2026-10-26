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
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Tests the {@code clusterMemberVersions} and {@code mixedVersionCluster} attributes of the Transport MBean.
 * <p>
 * The versions are read from the cluster view, where every {@link org.infinispan.remoting.transport.Address} carries
 * the version of the node it identifies, so the attributes are up to date as soon as the view is installed.
 * </p>
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
      assertEquals(expectedVersions(address(0).toString(), address(1).toString()), memberVersions(0));
      assertEquals(expectedVersions(address(0).toString(), address(1).toString()), memberVersions(1));
   }

   public void testHomogeneousClusterIsNotMixedVersion() {
      assertFalse(mixedVersionCluster(0));
      assertFalse(mixedVersionCluster(1));
   }

   public void testMemberVersionsFollowTheClusterView() {
      String address0 = address(0).toString();
      String address1 = address(1).toString();

      addNode();
      blockUntilViewsReceived();
      String address2 = address(2).toString();

      assertEquals(expectedVersions(address0, address1, address2), memberVersions(0));
      assertEquals(expectedVersions(address0, address1, address2), memberVersions(2));

      TestingUtil.killCacheManagers(manager(2));
      cacheManagers.remove(2);
      blockUntilViewsReceived();

      // the view is installed asynchronously on the surviving members
      eventuallyEquals(expectedVersions(address0, address1), () -> memberVersions(0));
      eventuallyEquals(expectedVersions(address0, address1), () -> memberVersions(1));
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

   /**
    * Every member of the view advertises the same {@code major.minor.patch} version in its address, including the one
    * whose MBean is read.
    */
   private Map<String, String> expectedVersions(String... addresses) {
      Map<String, String> expected = new HashMap<>();
      for (String address : addresses) {
         expected.put(address, NodeVersion.INSTANCE.toString());
      }
      return expected;
   }

   @SuppressWarnings("unchecked")
   private Map<String, String> memberVersions(int index) {
      return (Map<String, String>) attribute(index, "clusterMemberVersions");
   }

   private boolean mixedVersionCluster(int index) {
      return (Boolean) attribute(index, "mixedVersionCluster");
   }

   private Object attribute(int index, String attributeName) {
      try {
         MBeanServer server = mBeanServerLookup.getMBeanServer();
         return server.getAttribute(getCacheManagerObjectName(jmxDomain(index), "DefaultCacheManager", "Transport"),
               attributeName);
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   private String jmxDomain(int index) {
      return JMX_DOMAIN + index;
   }
}
