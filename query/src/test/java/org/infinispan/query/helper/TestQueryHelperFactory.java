/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.helper;

import junit.framework.Assert;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a test query helper
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 4.0
 */
public class TestQueryHelperFactory {
   
   public static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer(getLuceneVersion());
   
   public static QueryParser createQueryParser(String defaultFieldName) {
      return new QueryParser(getLuceneVersion(), defaultFieldName, STANDARD_ANALYZER);
   }
   
   public static Version getLuceneVersion() {
      return Version.LUCENE_30; //Change as needed
   }

   public static CacheQuery createCacheQuery(Cache m_cache, String fieldName, String searchString) throws ParseException {
      QueryParser qp = createQueryParser(fieldName);
      Query parsedQuery = qp.parse(searchString);
      SearchManager queryFactory = Search.getSearchManager(m_cache);
      CacheQuery cacheQuery = queryFactory.getQuery(parsedQuery);
      return cacheQuery;
   }
   
   public static SearchFactoryIntegrator extractSearchFactory(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      SearchFactoryIntegrator component = componentRegistry.getComponent(SearchFactoryIntegrator.class);
      Assert.assertNotNull(component);
      return component;
   }

   public static List createTopologyAwareCacheNodes(int numberOfNodes, CacheMode cacheMode, boolean transactional,
                                                    boolean indexLocalOnly, boolean isRamDirectoryProvider) {
      List caches = new ArrayList();

      ConfigurationBuilder builder = AbstractCacheTest.getDefaultClusteredCacheConfig(cacheMode, transactional);

      builder.indexing().enable().indexLocalOnly(indexLocalOnly);

      if(isRamDirectoryProvider) {
         builder.indexing().addProperty("default.directory_provider", "ram").addProperty("lucene_version", "LUCENE_CURRENT");
      } else {
         builder.indexing().addProperty("hibernate.search.default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
               .addProperty("default.directory_provider", "infinispan")
               .addProperty("hibernate.search.default.exclusive_index_use", "false")
               .addProperty("lucene_version", "LUCENE_36");
      }

      for(int i = 0; i < numberOfNodes; i++) {
         GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder
               .defaultClusteredBuilder();
         globalConfigurationBuilder.transport().machineId("a" + i).rackId("b" + i).siteId("test" + i);

         EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfigurationBuilder, builder);

         caches.add(cm1.getCache());
      }

      return caches;
   }
   
}
