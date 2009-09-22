package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.test.BrokenProvided;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

/**
 * This test is to try and create a searchable cache without the proper annotations used.
 *
 * @author Navin Surtani
 */
@Test (groups = "functional")
public class BrokenAnnotationTest extends SingleCacheManagerTest
{
   @Test (expectedExceptions = IllegalArgumentException.class)
   public void testProvided() throws Exception
   {
      org.infinispan.query.test.BrokenProvided provided = new org.infinispan.query.test.BrokenProvided();
      provided.setBoth("Cat", 5);
      
      Cache cache = createCacheManager().getCache();

      QueryHelper qh = new QueryHelper(cache, null, BrokenProvided.class);


   }

   @Test (expectedExceptions = IllegalArgumentException.class)
   public void testDocumentId() throws Exception
   {
      org.infinispan.query.test.BrokenDocumentId provided = new org.infinispan.query.test.BrokenDocumentId();
      provided.setBoth("Cat", 5);

      Cache cache = createCacheManager().getCache();

      QueryHelper qh = new QueryHelper(cache, null, BrokenProvided.class);



   }

   protected CacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager();  // TODO: Is there a better way to do this?
   }
}
