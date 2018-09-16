package org.infinispan.query.indexmanager;

import java.util.Properties;

import org.hibernate.search.backend.impl.lucene.WorkspaceHolder;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.WorkerBuildContext;

/**
 * Factory of local backends to simplify lazy initialization.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
class SimpleLocalBackendFactory implements LocalBackendFactory {

   private final IndexManager indexManager;
   private Properties cfg;
   private WorkerBuildContext buildContext;


   SimpleLocalBackendFactory(IndexManager indexManager, Properties cfg, WorkerBuildContext buildContext) {
      this.indexManager = indexManager;
      this.cfg = cfg;//TODO wrap to override some dangerous properties?
      this.buildContext = buildContext; //TODO deferring the buildContext in Hibernate Search is unexpected: think of a better way
   }

   @Override
   public IndexingBackend createLocalIndexingBackend() {
      WorkspaceHolder workspaceHolder = new WorkspaceHolder();
      workspaceHolder.initialize(cfg,buildContext,indexManager);
      return new LocalIndexingBackend(workspaceHolder);
   }

}
