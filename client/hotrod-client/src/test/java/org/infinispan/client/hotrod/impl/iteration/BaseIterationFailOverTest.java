package org.infinispan.client.hotrod.impl.iteration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional")
public abstract class BaseIterationFailOverTest extends MultiHotRodServersTest implements AbstractRemoteIteratorTest {

   protected final int SERVERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(SERVERS, getCacheConfiguration());
   }

   public abstract ConfigurationBuilder getCacheConfiguration();

   @Test
   public void testFailOver() throws InterruptedException {
      int cacheSize = 200;
      int batch = 17;
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();
      populateCache(cacheSize, this::newAccount, cache);

      List<Map.Entry<Object, Object>> entries = new ArrayList<>();
      CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, batch);
      for (int i = 0; i < cacheSize / 2; i++) {
         Map.Entry<Object, Object> next = iterator.next();
         entries.add(next);
      }

      killIterationServer();

      while (iterator.hasNext()) {
         Map.Entry<Object, Object> next = iterator.next();
         entries.add(next);

      }

      assertEquals(cacheSize, entries.size());

      Set<Integer> keys = extractKeys(entries);
      assertEquals(rangeAsSet(0, cacheSize), keys);

   }

   private void killIterationServer() {
      servers.stream()
            .filter(s -> s.iterationManager().activeIterations() > 0)
            .forEach(HotRodClientTestingUtil::killServers);
   }


}
