package org.infinispan.query.indexmanager;

import java.util.List;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.impl.lucene.WorkspaceHolder;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The IndexingBackend which directly couples to the Hibernate Search WorkspaceHolder.
 * Normally this will be the "lucene" backend, writing to the index.
 *
 * @see org.hibernate.search.backend.impl.lucene.WorkspaceHolder
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
final class LocalIndexingBackend implements IndexingBackend {

   private static final Log log = LogFactory.getLog(LocalIndexingBackend.class, Log.class);
   private final WorkspaceHolder workspaceHolder;

   LocalIndexingBackend(WorkspaceHolder workspaceHolder) {
      this.workspaceHolder = workspaceHolder;
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, IndexManager indexManager) {
      log.applyingChangeListLocally(workList);
      workspaceHolder.applyWork(workList, monitor);
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, IndexManager indexManager) {
      workspaceHolder.applyStreamWork(singleOperation, monitor);
   }

   @Override
   public boolean isMasterLocal() {
      return true;
   }

   @Override
   public void flushAndClose(IndexingBackend replacement) {
      workspaceHolder.close();
      log.debug("Downgraded from Master role: Index lock released.");
   }

   public String toString() {
      return "LocalIndexingBackend";
   }

}
