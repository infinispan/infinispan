package org.infinispan.query.distributed;

import java.io.Serializable;
import java.util.List;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.testng.annotations.Test;

/**
 * Tests for entities sharing the same index in DIST caches.
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.OverlappingDistMassIndexTest")
public class OverlappingDistMassIndexTest extends OverlappingIndexMassIndexTest {

   @Override
   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.LOCAL)
            .addIndexedEntity(Transaction.class)
            .addIndexedEntity(Block.class)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      List<Cache<String, Object>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      waitForClusterToForm(BasicCacheContainer.DEFAULT_CACHE_NAME);

      for (Cache cache : cacheList) {
         caches.add(cache);
      }
   }
}

@Indexed(index = "commonIndex")
class Transaction implements Serializable {

   @Field(analyze = Analyze.NO)
   int size;

   @Field
   String script;

   public Transaction(int size, String script) {
      this.size = size;
      this.script = script;
   }

   @Override
   public String toString() {
      return "Transaction{" +
            "size=" + size +
            ", script='" + script + '\'' +
            '}';
   }
}

@Indexed(index = "commonIndex")
class Block implements Serializable {

   @Field(analyze = Analyze.NO)
   int height;

   @IndexedEmbedded
   Transaction latest;

   public Block(int height, Transaction latest) {
      this.height = height;
      this.latest = latest;
   }

   @Override
   public String toString() {
      return "Block{" +
            "height=" + height +
            ", latest=" + latest +
            '}';
   }
}
