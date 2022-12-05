package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests stats operation against a Hot Rod server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodStatsTest")
public class HotRodStatsTest extends HotRodSingleNodeTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   public EmbeddedCacheManager createTestCacheManager() {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.statistics().enable();
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalCfg.cacheContainer().statistics(true).metrics().accurateSize(true);
      configureJmx(globalCfg, jmxDomain(), mBeanServerLookup);
      return TestCacheManagerFactory.createClusteredCacheManager(globalCfg, cfg);
   }

   public void testStats(Method m) {
      int bytesRead = 0;
      int bytesWritten = 0;

      Map<String, String> s = client().stats();
      assertEquals(s.get("currentNumberOfEntries"), "0");
      assertEquals(s.get("stores"), "0");
      assertEquals(s.get("retrievals"), "0");
      assertEquals(s.get("hits"), "0");
      assertEquals(s.get("misses"), "0");
      assertEquals(s.get("removeHits"), "0");
      assertEquals(s.get("removeMisses"), "0");
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"));
      // At time of request, no data had been written yet
      assertEquals(s.get("totalBytesWritten"), "0");

      client().assertPut(m);
      s = client().stats();
      assertEquals(s.get("currentNumberOfEntries"), "1");
      assertEquals(s.get("stores"), "1");
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"));
      bytesWritten = assertHigherBytes(bytesWritten, s.get("totalBytesWritten"));

      assertFalse(Objects.equals(s.get("totalBytesRead"), "0"));
      assertFalse(Objects.equals(s.get("totalBytesWritten"), "0"));

      assertSuccess(client().assertGet(m), v(m));
      s = client().stats();
      assertEquals(s.get("hits"), "1");
      assertEquals(s.get("misses"), "0");
      assertEquals(s.get("retrievals"), "1");
      assertHigherBytes(bytesRead, s.get("totalBytesRead"));
      assertHigherBytes(bytesWritten, s.get("totalBytesWritten"));

      client().clear();

      s = client().stats();
      assertEquals(s.get("currentNumberOfEntries"), "0");
   }

   private int assertHigherBytes(int currentBytesRead, String bytesStr) {
      int bytesRead = Integer.valueOf(bytesStr);
      assertTrue(String.format("Expecting %d > %d", bytesRead, currentBytesRead), bytesRead > currentBytesRead);
      return bytesRead;
   }
}
