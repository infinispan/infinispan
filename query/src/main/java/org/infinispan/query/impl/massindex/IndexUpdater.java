package org.infinispan.query.impl.massindex;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.logging.Log;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;
import org.infinispan.util.logging.LogFactory;

/**
 * Handle batch updates to an index.
 *
 * @author gustavonalle
 * @since 7.1
 */
public class IndexUpdater {

   private static final Log LOG = LogFactory.getLog(IndexUpdater.class, Log.class);

   private final SearchMappingHolder searchMappingHolder;
   private final KeyTransformationHandler keyTransformationHandler;

   public IndexUpdater(SearchMappingHolder searchMappingHolder, KeyTransformationHandler keyTransformationHandler) {
      this.searchMappingHolder = searchMappingHolder;
      this.keyTransformationHandler = keyTransformationHandler;
   }

   public void flush(Collection<Class<?>> javaClasses) {
      if (javaClasses.isEmpty()) {
         return;
      }

      LOG.flushingIndex(javaClasses.toString());
      searchMappingHolder.getSearchMapping().scopeFromJavaClasses(javaClasses).workspace().flush();
   }

   public void refresh(Collection<Class<?>> javaClasses) {
      if (javaClasses.isEmpty()) {
         return;
      }

      LOG.flushingIndex(javaClasses.toString());
      searchMappingHolder.getSearchMapping().scopeFromJavaClasses(javaClasses).workspace().refresh();
   }

   public void purge(Collection<Class<?>> javaClasses) {
      if (javaClasses.isEmpty()) {
         return;
      }

      LOG.purgingIndex(javaClasses.toString());
      searchMappingHolder.getSearchMapping().scopeFromJavaClasses(javaClasses).workspace().purge();
   }

   public Collection<Class<?>> allJavaClasses() {
      return searchMappingHolder.getSearchMapping().allIndexedTypes().values();
   }

   public CompletableFuture<?> updateIndex(Object key, Object value, int segment) {
      if (value == null || Thread.currentThread().isInterrupted()) {
         return CompletableFuture.completedFuture(null);
      }

      final String idInString = keyTransformationHandler.keyToString(key, segment);
      return searchMappingHolder.getSearchMapping().getSearchIndexer().addOrUpdate(idInString, value);
   }
}
