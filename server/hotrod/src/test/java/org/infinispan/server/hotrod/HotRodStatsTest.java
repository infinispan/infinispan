package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Map;

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
      assertEquals("0", s.get("currentNumberOfEntries"));
      assertEquals("0", s.get("stores"));
      assertEquals("0", s.get("retrievals"));
      assertEquals("0", s.get("hits"));
      assertEquals("0", s.get("misses"));
      assertEquals("0", s.get("removeHits"));
      assertEquals("0", s.get("removeMisses"));
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"));
      // At time of request, we have only done ping
      bytesWritten = Integer.parseInt(s.get("totalBytesWritten"));
      assertTrue(bytesWritten == 5 || bytesWritten == 6, String.format("Expecting %d in [5,6]", bytesWritten));

      client().assertPut(m);
      s = client().stats();
      assertEquals("1", s.get("currentNumberOfEntries"));
      assertEquals("1", s.get("stores"));
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"));
      bytesWritten = assertHigherBytes(bytesWritten, s.get("totalBytesWritten"));

      assertNotEquals("0", s.get("totalBytesRead"));
      assertNotEquals("0", s.get("totalBytesWritten"));

      assertSuccess(client().assertGet(m), v(m));
      s = client().stats();
      assertEquals("1", s.get("hits"));
      assertEquals("0", s.get("misses"));
      assertEquals("1", s.get("retrievals"));
      assertHigherBytes(bytesRead, s.get("totalBytesRead"));
      assertHigherBytes(bytesWritten, s.get("totalBytesWritten"));

      client().clear();

      s = client().stats();
      assertEquals("0", s.get("currentNumberOfEntries"));
   }

   private int assertHigherBytes(int currentBytesRead, String bytesStr) {
      int bytesRead = Integer.parseInt(bytesStr);
      assertTrue(bytesRead > currentBytesRead, String.format("Expecting %d > %d", bytesRead, currentBytesRead));
      return bytesRead;
   }
}
