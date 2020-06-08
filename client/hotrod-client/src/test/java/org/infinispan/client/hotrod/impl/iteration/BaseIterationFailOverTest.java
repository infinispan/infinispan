package org.infinispan.client.hotrod.impl.iteration;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.embedded.DslSCI;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
public abstract class BaseIterationFailOverTest extends MultiHotRodServersTest implements AbstractRemoteIteratorTest {

   protected final int SERVERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(SERVERS, getCacheConfiguration());
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return DslSCI.INSTANCE;
   }

   public abstract ConfigurationBuilder getCacheConfiguration();

   @Override
   protected int maxRetries() {
      // We kill one node in many tests - let it failover in that case
      return 1;
   }

   @Test(groups = "functional")
   public void testFailOver() throws InterruptedException {
      int cacheSize = 1_000;
      int batch = 17;
      RemoteCache<Integer, AccountHS> cache = clients.get(0).getCache();
      populateCache(cacheSize, this::newAccount, cache);

      List<Map.Entry<Object, Object>> entries = new ArrayList<>();
      CloseableIterator<Map.Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, batch);
      for (int i = 0; i < cacheSize / 2; i++) {
         Map.Entry<Object, Object> next = iterator.next();
         entries.add(next);
      }

      killAnIterationServer();

      while (iterator.hasNext()) {
         Map.Entry<Object, Object> next = iterator.next();
         entries.add(next);

      }

      assertEquals(cacheSize, entries.size());

      Set<Integer> keys = extractKeys(entries);
      assertEquals(rangeAsSet(0, cacheSize), keys);
   }

   protected void killAnIterationServer() {
      servers.stream()
            .filter(s -> s.getIterationManager().activeIterations() > 0)
            .findFirst().ifPresent(HotRodClientTestingUtil::killServers);
   }
}
