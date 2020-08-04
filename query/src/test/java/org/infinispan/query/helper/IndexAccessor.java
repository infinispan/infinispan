package org.infinispan.query.helper;

import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.hibernate.search.backend.lucene.index.LuceneIndexManager;
import org.hibernate.search.backend.lucene.index.impl.LuceneIndexManagerImpl;
import org.hibernate.search.backend.lucene.index.impl.Shard;
import org.hibernate.search.backend.lucene.lowlevel.index.impl.IndexAccessorImpl;
import org.hibernate.search.engine.backend.index.IndexManager;
import org.hibernate.search.engine.common.spi.SearchIntegration;
import org.hibernate.search.engine.environment.bean.BeanHolder;
import org.hibernate.search.engine.reporting.FailureHandler;
import org.hibernate.search.engine.reporting.impl.FailSafeFailureHandlerWrapper;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.infinispan.Cache;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;

/**
 * Provides some low level api index instances.
 * <p>
 * For tests only.
 *
 * @author Fabio Massimo Ercoli
 */
public class IndexAccessor {

   private final LuceneIndexManagerImpl indexManager;
   private final List<Shard> shardsForTests;
   private final IndexAccessorImpl indexAccessor;

   public static IndexAccessor of(Cache<?, ?> cache, Class<?> entityType) {
      return new IndexAccessor(cache, entityType);
   }

   public IndexAccessor(Cache<?, ?> cache, Class<?> entityType) {
      SearchMapping searchMapping = TestQueryHelperFactory.extractSearchMapping(cache);
      SearchIndexedEntity searchIndexedEntity = searchMapping.indexedEntity(entityType);
      if (searchIndexedEntity == null) {
         fail("Entity " + entityType + " is not indexed.");
      }

      indexManager = (LuceneIndexManagerImpl) searchIndexedEntity.indexManager().unwrap(LuceneIndexManager.class);
      shardsForTests = indexManager.getShardsForTests();
      indexAccessor = shardsForTests.get(0).getIndexAccessorForTests();
   }

   public IndexManager getIndexManager() {
      return indexManager;
   }

   public List<Shard> getShardsForTests() {
      return shardsForTests;
   }

   /**
    * Provides a Lucene index reader.
    * The instance is not supposed to be closed by the caller.
    * Hibernate Search will take care of it.
    *
    * @return a Lucene index reader
    */
   public DirectoryReader getIndexReader() {
      try {
         return indexAccessor.getIndexReader();
      } catch (IOException e) {
         throw new RuntimeException("Cannot get index reader");
      }
   }

   public Directory getDirectory() {
      return indexAccessor.getDirectoryForTests();
   }

   public static Sort extractSort(SearchQuery<?> searchQuery) {
      return (Sort) ReflectionUtil.getValue(searchQuery, "luceneSort");
   }

   public static FailureHandler extractFailureHandler(Cache<?, ?> cache) {
      SearchIntegration searchIntegration = (SearchIntegration)
            ReflectionUtil.getValue(TestQueryHelperFactory.extractSearchMapping(cache), "integration");
      BeanHolder<? extends FailureHandler> failureHandlerHolder = (BeanHolder<? extends FailureHandler>)
            ReflectionUtil.getValue(searchIntegration, "failureHandlerHolder");

      FailureHandler failureHandler = failureHandlerHolder.get();
      if (failureHandler instanceof FailSafeFailureHandlerWrapper) {
         return (FailureHandler) ReflectionUtil.getValue(failureHandler, "delegate");
      }
      return failureHandler;
   }
}
