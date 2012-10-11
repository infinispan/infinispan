package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Adding another configuration to BookSearchTest so that the keys and values are stored binary in the cache.
 * The test verifies the issue ISPN-2138, i.e. no ClassCasting exception should be thrown.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.BookStoreAsBinarySearchTest")
public class BookStoreAsBinarySearchTest extends BookSearchTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);

      //Verification for ISPN-2138 - the following exception should not appear:
      //java.lang.ClassCastException: org.infinispan.marshall.MarshalledValue cannot be cast to org.infinispan.distexec.mapreduce.Book
      builder.storeAsBinary().enable();
      createClusteredCaches(4, "bookSearch", builder);
   }

}
