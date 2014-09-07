package org.infinispan.query.indexmanager;

import java.util.List;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;

/**
 * This backend only triggers initialization of a different backend when
 * incoming indexing operations trigger it, then transfers the incoming
 * operations to the new backend.
 * Which backed is being selected depends on the cluster state.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
public class LazyInitializingBackend implements IndexingBackend {

   private final LazyInitializableBackend backend;

   public LazyInitializingBackend(LazyInitializableBackend backend) {
      this.backend = backend;
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      backend.lazyInitialize();
      backend.getCurrentIndexingBackend().applyWork(workList, monitor, indexManager);
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager) {
      backend.lazyInitialize();
      backend.getCurrentIndexingBackend().applyStreamWork(singleOperation, monitor, indexManager);

   }

   @Override
   public boolean isMasterLocal() {
      // Avoid initializing yet
      return false;
   }

   @Override
   public void flushAndClose(IndexingBackend replacement) {
      //no-op: this is essentially stateless
   }

   public String toString() {
      return "LazyInitializingBackend";
   }

}
