package org.infinispan.client.hotrod.query.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.FILESYSTEM;
import static org.infinispan.query.aggregation.QueryAggregationExpiringTest.resultMaps;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Task;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.aggregation.RemoteClusteredAggregationExpiringTest")
@TestForIssue(githubKey = "13194")
public class RemoteClusteredAggregationExpiringTest extends MultiHotRodServersTest {

   private final String indexDirectory = CommonsTestingUtil.tmpDirectory(getClass());
   private final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(indexDirectory);
      assertThat(new File(indexDirectory).mkdirs()).isTrue();

      ConfigurationBuilder config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      config.clustering()
       .cacheMode(CacheMode.DIST_SYNC)
            .remoteTimeout(17500)
            .hash().numOwners(1);
      config.persistence().passivation(false)
            .addStore(new DummyInMemoryStoreConfigurationBuilder(config.persistence()));
      config.indexing().enable()
            .storage(FILESYSTEM).path(indexDirectory)
            .addIndexedEntity("model.Task");

      createHotRodServers(1, config);
      waitForClusterToForm();
      TestingUtil.replaceComponent(manager(0), TimeService.class, timeService, true);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Task.TaskSchema.INSTANCE;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      try {
         //first stop cache managers, then clear the index
         super.destroy();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         Util.recursiveFileRemove(indexDirectory);
      }
      super.destroy();
   }

   @Test
   public void test() throws Exception {
      RemoteCache<Integer, Task> remoteCache = clients.get(0).getCache();
      for (int i=0; i<12; i++) {
         int module = i % 4;
         remoteCache.put(i, new Task(100 + i, "type-" + module, "status-" + module, "label-" + module),
               1, TimeUnit.SECONDS);
         timeService.advance(10, TimeUnit.MILLISECONDS);
      }

      timeService.advance(500, TimeUnit.MILLISECONDS);
      Map<String, Long> result;
      do {
         timeService.advance(100, TimeUnit.MILLISECONDS);
         Query<Object[]> query = remoteCache.query("select status, count(status) from model.Task group by status");
         result = resultMaps(query.list());
         assertThat(result).isNotNull();

         query = remoteCache.query("select label, count(label) from model.Task group by label");
         result = resultMaps(query.list());
         assertThat(result).isNotNull();

         query = remoteCache.query("select type, count(type) from model.Task group by type");
         result = resultMaps(query.list());
         assertThat(result).isNotNull();
      } while (!result.isEmpty());
   }
}
