/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.programmaticmapping;

import java.lang.annotation.ElementType;
import java.util.Properties;

import org.apache.lucene.search.Query;
import org.hibernate.search.Environment;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.cfg.SearchMapping;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verify programmatic configuration of indexing properties via SearchMapping
 * is properly enabled in the Search engine. See also ISPN-1820.
 * 
 * @author Michael Wittig
 * @author Sanne Grinovero
 * @since 5.1.1
 */
public class SearchMappingTest {

   /**
    * Here we use SearchMapping to have the ability to add Objects to the cache
    * where people can not (or don't want to) use Annotations.
    */
   @Test
   public void testSearchMapping() {
      final SearchMapping mapping = new SearchMapping();
      mapping.entity(BondPVO.class).indexed().providedId()
            .property("id", ElementType.METHOD).field()
            .property("name", ElementType.METHOD).field()
            .property("isin", ElementType.METHOD).field();

      final Properties properties = new Properties();
      properties.put("hibernate.search.default.directory_provider", "ram");
      properties.put(Environment.MODEL_MAPPING, mapping);

      final Configuration config = new ConfigurationBuilder().indexing()
            .enable().indexLocalOnly(true).withProperties(properties).build();

      final DefaultCacheManager cacheManager = new DefaultCacheManager(config);
      try {
         final Cache<Long, BondPVO> cache = cacheManager.getCache();
         final SearchManager sm = Search.getSearchManager(cache);

         final BondPVO bond = new BondPVO(1, "Test", "DE000123");
         cache.put(bond.getId(), bond);

         final QueryBuilder qb = sm.buildQueryBuilderForClass(BondPVO.class).get();
         final Query q = qb.keyword().onField("name").matching("Test")
            .createQuery();
         final CacheQuery cq = sm.getQuery(q, BondPVO.class);
         Assert.assertEquals(cq.getResultSize(), 1);
      }
      finally {
         cacheManager.stop();
      }
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
      properties.put("hibernate.search.default.directory_provider", "ram");

      final Configuration config = new ConfigurationBuilder().indexing()
            .enable().indexLocalOnly(true).withProperties(properties).build();

      final DefaultCacheManager cacheManager = new DefaultCacheManager(config);
      try {
         final Cache<Long, BondPVO2> cache = cacheManager.getCache();
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
      finally {
         cacheManager.stop();
      }
   }

   @Indexed
   @ProvidedId
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
