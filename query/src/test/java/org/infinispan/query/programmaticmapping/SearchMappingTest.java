package org.infinispan.query.programmaticmapping;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.lang.annotation.ElementType;
import java.util.Properties;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.cfg.Environment;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verify programmatic configuration of indexing properties via SearchMapping is properly enabled in the Search engine.
 * See also ISPN-1820.
 *
 * @author Michael Wittig
 * @author Sanne Grinovero
 * @since 5.1.1
 */
@Test(groups = "functional", testName = "query.programmaticmapping.SearchMappingTest")
public class SearchMappingTest extends AbstractInfinispanTest {

   /**
    * Here we use SearchMapping to have the ability to add Objects to the cache where people can not (or don't want to)
    * use Annotations.
    */
   @Test
   public void testSearchMapping() {
      SearchMapping mapping = new SearchMapping();
      mapping.entity(BondPVO.class).indexed()
            .property("id", ElementType.METHOD).field()
            .property("name", ElementType.METHOD).field()
            .property("isin", ElementType.METHOD).field();

      Properties properties = new Properties();
      properties.put("default.directory_provider", "local-heap");
      properties.put("lucene_version", "LUCENE_CURRENT");
      properties.put(Environment.MODEL_MAPPING, mapping);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing()
             .enable()
             .addIndexedEntity(BondPVO.class)
             .withProperties(properties);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<Long, BondPVO> cache = cm.getCache();
            QueryFactory queryFactory = Search.getQueryFactory(cache);

            BondPVO bond = new BondPVO(1, "Test", "DE000123");
            cache.put(bond.getId(), bond);

            String q = String.format("FROM %s WHERE name:'Test'", BondPVO.class.getName());
            Query cq = queryFactory.create(q);
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
      Properties properties = new Properties();
      properties.put("default.directory_provider", "local-heap");
      properties.put("lucene_version", "LUCENE_CURRENT");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing()
             .enable()
             .addIndexedEntity(BondPVO2.class)
             .withProperties(properties);

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            Cache<Long, BondPVO2> cache = cm.getCache();
            QueryFactory queryFactory = Search.getQueryFactory(cache);

            BondPVO2 bond = new BondPVO2(1, "Test", "DE000123");
            cache.put(bond.getId(), bond);
            String q = String.format("FROM %s WHERE name:'Test'", BondPVO2.class.getName());
            Query cq = queryFactory.create(q);
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
