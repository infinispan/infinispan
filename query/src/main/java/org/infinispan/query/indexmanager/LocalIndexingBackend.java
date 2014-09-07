package org.infinispan.query.indexmanager;

import java.util.List;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.spi.BackendQueueProcessor;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The IndexingBackend which directly couples to the Hibernate Search backend.
 * Normally this will be the "lucene" backend, writing to the index.
 *
 * @see org.hibernate.search.backend.impl.lucene.LuceneBackendQueueProcessor
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class LocalIndexingBackend implements IndexingBackend {

   private static final Log log = LogFactory.getLog(LocalIndexingBackend.class, Log.class);
   private final BackendQueueProcessor localBackend;

   public LocalIndexingBackend(BackendQueueProcessor localBackend) {
      this.localBackend = localBackend;
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      log.applyingChangeListLocally(workList);
      localBackend.applyWork(workList, monitor);
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      localBackend.applyStreamWork(singleOperation, monitor);
   }

   @Override
   public boolean isMasterLocal() {
      return true;
   }

   @Override
   public void flushAndClose(IndexingBackend replacement) {
      localBackend.close();
      log.debug("Downgraded from Master role: Index lock released.");
   }

   public String toString() {
      return "LocalIndexingBackend";
   }

}
