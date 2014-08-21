package org.infinispan.query.indexmanager;

import java.util.Properties;

import org.hibernate.search.backend.BackendFactory;
import org.hibernate.search.backend.spi.BackendQueueProcessor;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.hibernate.search.spi.WorkerBuildContext;

/**
 * Factory of local backends to simplify lazy initialization.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
public class SimpleLocalBackendFactory implements LocalBackendFactory {

   private final DirectoryBasedIndexManager indexManager;
   private Properties cfg;
   private WorkerBuildContext buildContext;
   

   SimpleLocalBackendFactory(DirectoryBasedIndexManager indexManager, Properties cfg, WorkerBuildContext buildContext) {
      this.indexManager = indexManager;
      this.cfg = cfg;//TODO wrap to override some dangerous properties?
      this.buildContext = buildContext; //TODO deferring the buildContext in Hibernate Search is unexpected: think of a better way
   }

   @Override
   public IndexingBackend createLocalIndexingBackend() {
      //Force to use the "lucene" backend?
      BackendQueueProcessor localBackend = BackendFactory.createBackend(indexManager, buildContext, cfg);
      IndexingBackend backend = new LocalIndexingBackend(localBackend);
      return backend;
   }

}
