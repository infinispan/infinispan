package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LookupMode;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * TwoNodesWithStoreMapReduceTest tests Map/Reduce functionality using two Infinispan nodes,
 * local reduce and also to verify that having values in cache store only does not lead to any
 * additional key/value being missed for map/reduce algorithm
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Test(groups = "functional", testName = "distexec.mapreduce.TwoNodesWithStoreMapReduceTest")
public class TwoNodesWithStoreMapReduceTest extends BaseWordCountMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(getClass().getSimpleName()).purgeOnStartup(true);
      createClusteredCaches(2, cacheName(), builder);
   }

   @Override
   @SuppressWarnings({ "rawtypes"})
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[],
            Mapper<String, String, String, Integer> mapper, Reducer<String, Integer> reducer, boolean useCombiner)
            throws Exception {
      Cache cache1 = cache(0, cacheName());
      Cache cache2 = cache(1, cacheName());

      CacheWriter c1 = (CacheWriter) TestingUtil.getCacheLoader(cache1);
      CacheWriter c2 = (CacheWriter) TestingUtil.getCacheLoader(cache2);

      //store each entry into each cache store directly
      write("1", "Hello world here I am");
      write("1", "Hello world here I am");
      write("2", "Infinispan rules the world");
      write("2", "Infinispan rules the world");
      write("3", "JUDCon is in Boston");
      write("3", "JUDCon is in Boston");
      write("4", "JBoss World is in Boston as well");
      write("4", "JBoss World is in Boston as well");
      write("12", "JBoss Application Server");
      write("12", "JBoss Application Server");
      write("15", "Hello world");
      write("15", "Hello world");
      write("14", "Infinispan community");
      write("14", "Infinispan community");

      write("111", "Infinispan open source");
      write("111", "Infinispan open source");
      write("112", "Boston is close to Toronto");
      write("112", "Boston is close to Toronto");
      write("113", "Toronto is a capital of Ontario");
      write("113", "Toronto is a capital of Ontario");
      write("114", "JUDCon is cool");
      write("114", "JUDCon is cool");
      write("211", "JBoss World is awesome");
      write("211", "JBoss World is awesome");
      write("212", "JBoss rules");
      write("212", "JBoss rules");
      write("213", "JBoss division of RedHat ");
      write("213", "JBoss division of RedHat ");
      write("214", "RedHat community");
      write("214", "RedHat community");

      MapReduceTask<String, String, String, Integer> task = createMapReduceTask(cache1);
      task.mappedWith(mapper).reducedWith(reducer);
      if(useCombiner)
         task.combinedWith(reducer);

      if(keys != null && keys.length>0){
         task.onKeys(keys);
      }
      return task;
   }

   private void write(String key, Object value) {
      Cache cache1 = cache(0, cacheName());
      ClusteringDependentLogic cdl = cache1.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
      boolean onCache1 = cdl.localNodeIsPrimaryOwner(key, LookupMode.WRITE);
      CacheWriter cacheWriter;
      if (onCache1) {
         cacheWriter = (CacheWriter) TestingUtil.getCacheLoader(cache1);
      } else {
         cacheWriter = (CacheWriter) TestingUtil.getCacheLoader(cache(1, cacheName()));
      }
      cacheWriter.write(new MarshalledEntryImpl(key, value, null, TestingUtil.marshaller(cache1)));
   }

   @Override
   public void testInvokeMapReduceOnSubsetOfKeys() throws Exception {
      super.testInvokeMapReduceOnSubsetOfKeys();    // TODO: Customise this generated block
   }
}
