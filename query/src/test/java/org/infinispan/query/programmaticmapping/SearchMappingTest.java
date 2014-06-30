package org.infinispan.query.programmaticmapping;

import java.lang.annotation.ElementType;
import java.util.Properties;

import org.apache.lucene.search.Query;
import org.hibernate.search.cfg.Environment;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Verify programmatic configuration of indexing properties via SearchMapping
 * is properly enabled in the Search engine. See also ISPN-1820.
 * 
 * @author Michael Wittig
 * @author Sanne Grinovero
 * @since 5.1.1
 */
@Test(groups = "functional", testName = "query.programmaticmapping.SearchMappingTest")
public class SearchMappingTest {

   /**
    * Here we use SearchMapping to have the ability to add Objects to the cache
    * where people can not (or don't want to) use Annotations.
    */
   @Test
   public void testSearchMapping() {
      final SearchMapping mapping = new SearchMapping();
      mapping.entity(BondPVO.class).indexed()
            .property("id", ElementType.METHOD).field()
            .property("name", ElementType.METHOD).field()
            .property("isin", ElementType.METHOD).field();

      final Properties properties = new Properties();
      properties.put("default.directory_provider", "ram");
      properties.put("lucene_version", "LUCENE_CURRENT");
      properties.put(Environment.MODEL_MAPPING, mapping);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing()
            .index(Index.LOCAL).withProperties(properties);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            final Cache<Long, BondPVO> cache = cm.getCache();
            final SearchManager sm = Search.getSearchManager(cache);

            final BondPVO bond = new BondPVO(1, "Test", "DE000123");
            cache.put(bond.getId(), bond);

            final QueryBuilder qb = sm.buildQueryBuilderForClass(BondPVO.class).get();
            final Query q = qb.keyword().onField("name").matching("Test")
                  .createQuery();
            final CacheQuery cq = sm.getQuery(q, BondPVO.class);
            Assert.assertEquals(cq.getResultSize(), 1);
         }
      });
   }

   public static final class BondPVO {

      private long id;
      private String name;
      private String isin;

      public BondPVO(long id, String name, String isin) {
         this.id = id;
         this.name = name;
         this.isin = isin;
      }

      public long getId() {
         return id;
      }

      public void setId(long id) {
         this.id = id;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getIsin() {
         return isin;
      }

      public void setIsin(String isin) {
         this.isin = isin;
      }
   }

   /**
    * Here we show that the first test could work.
    */
   @Test
   public void testWithoutSearchMapping() {
      final Properties properties = new Properties();
      properties.put("default.directory_provider", "ram");
      properties.put("lucene_version", "LUCENE_CURRENT");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().index(Index.LOCAL).withProperties(properties);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            final Cache<Long, BondPVO2> cache = cm.getCache();
            final SearchManager sm = Search.getSearchManager(cache);

            final BondPVO2 bond = new BondPVO2(1, "Test", "DE000123");
            cache.put(bond.getId(), bond);
            final QueryBuilder qb = sm.buildQueryBuilderForClass(BondPVO2.class)
                  .get();
            final Query q = qb.keyword().onField("name").matching("Test")
                  .createQuery();
            final CacheQuery cq = sm.getQuery(q, BondPVO2.class);
            Assert.assertEquals(cq.getResultSize(), 1);
         }
      });
   }

   @Indexed
   public static final class BondPVO2 {

      @Field
      private long id;

      @Field
      private String name;

      @Field
      private String isin;

      public BondPVO2(long id, String name, String isin) {
         this.id = id;
         this.name = name;
         this.isin = isin;
      }

      public long getId() {
         return id;
      }

      public void setId(long id) {
         this.id = id;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getIsin() {
         return isin;
      }

      public void setIsin(String isin) {
         this.isin = isin;
      }
   }
}
