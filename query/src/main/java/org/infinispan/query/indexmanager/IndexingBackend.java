package org.infinispan.query.indexmanager;

import java.util.List;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;

/**
 * The main IndexingBackend implementations are the one forwarding to another node, and the one applying to the local node.
 * We when defined a set of additional implementations to handle intermediate transitionaly behaviour, each such implementation
 * respects this contract.
 * 
 * @see ClusteredSwitchingBackend
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
interface IndexingBackend {

   /**
    * Signals that the current implementation is being discarded, and if any pending update
    * operations are being buffered these should be delegated for further processing to the
    * replacement IndexingBackend.
    * @param replacement the new IndexingBackend taking the place of this.
    */
   void flushAndClose(IndexingBackend replacement);

   /**
    * Receives transactional index update operations from the Search engine.
    */
   void applyWork(List<LuceneWork> workList, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager);

   /**
    * Receives stream-style index update operations from the Search engine.
    */
   void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor, DirectoryBasedIndexManager indexManager);

   /**
    * Mostly useful for testing and diagnostics.
    * @return {@code true} if this node is actively able to write to the index, without delegating or postponing the write operations.
    */
   boolean isMasterLocal();

}
