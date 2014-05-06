package org.infinispan.it.osgi.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class DistributedTwoNodesMapReduceTest extends BaseWordCountMapReduceTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      //not used
   }

   @Before
   public void setUp() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      builder.clustering().stateTransfer().chunkSize(2);
      createClusteredCaches(2, cacheName(), builder);
   }

   @After
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManagers);
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   protected MapReduceTask<String, String, String, Integer> createMapReduceTask(Cache c){
      //run distributed reduce with per task cache
      return new MapReduceTask<String, String, String, Integer>(c, true, false);
   }
}
