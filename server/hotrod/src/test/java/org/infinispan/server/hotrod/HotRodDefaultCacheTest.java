package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertFalse;

import java.lang.reflect.Method;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.TestResponse;
import org.testng.annotations.Test;

/**
 * Test class for setting an alternate default cache
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodDefaultCacheTest")
public class HotRodDefaultCacheTest extends HotRodSingleNodeTest {
   private static final String ANOTHER_CACHE = "AnotherCache";

   @Override
   public HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      cacheManager.defineConfiguration(ANOTHER_CACHE, cacheManager.getDefaultCacheConfiguration());
      return startHotRodServer(cacheManager, ANOTHER_CACHE);
   }

   public void testPutOnDefaultCache(Method m) {
      TestResponse resp = client().execute(0xA0, (byte) 0x01, "", k(m), 0, 0, v(m), 0, (byte) 1, 0);
      assertStatus(resp, Success);
      assertHotRodEquals(cacheManager, ANOTHER_CACHE, k(m), v(m));
      assertFalse(cacheManager.getCache().containsKey(k(m)));
   }
}
