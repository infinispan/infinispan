package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Adding another configuration to BookSearchTest so that the keys and values are stored binary in the cache.
 * The cache is configured with eviction and passivation is done to cache store.
 *
 * The test verifies the issue ISPN-2386, i.e. no ClassCastException should be thrown.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.mapreduce.BookStoreAsBinaryWithStoreSearchTest")
public class BookStoreAsBinaryWithStoreSearchTest extends BookSearchTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);

      //Verification for ISPN-2386 - the following exception should not appear:
      //java.lang.ClassCastException: org.infinispan.marshall.MarshalledValue cannot be cast to org.infinispan.distexec.mapreduce.Book

      builder.eviction().maxEntries(1).strategy(EvictionStrategy.LRU);
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(getClass().getSimpleName());
      builder.storeAsBinary().enable();

      createClusteredCaches(4, "bookSearch", builder);
   }

}
