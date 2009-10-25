package org.infinispan.query.blackbox;

import org.testng.annotations.Test;
import org.infinispan.config.Configuration;
import org.infinispan.query.test.Person;
import org.infinispan.Cache;

import java.util.List;

/**
 * Clustered version of {@link org.infinispan.query.blackbox.MarshalledValueQueryTest}
 *
 *
 * @author Navin Surtani
 * @since 4.0
 */


@Test (groups = "functional")
public class MarshalledValueClusteredQueryTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers(){

      Configuration cacheCfg = new Configuration();
      cacheCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cacheCfg.setFetchInMemoryState(false);
      cacheCfg.setUseLazyDeserialization(true);

      List<Cache<String, Person>> caches = createClusteredCaches(2, "infinispan-query", cacheCfg);

      cache1 = caches.get(0);
      cache2 = caches.get(1);

   }

}
