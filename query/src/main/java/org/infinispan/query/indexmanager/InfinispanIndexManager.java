package org.infinispan.query.indexmanager;

import java.util.Properties;

import org.hibernate.search.backend.impl.lucene.WorkspaceHolder;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.WorkerBuildContext;
import org.hibernate.search.store.DirectoryProvider;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A custom IndexManager to store indexes in the grid itself.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
public class InfinispanIndexManager extends DirectoryBasedIndexManager {

   private static final Log log = LogFactory.getLog(InfinispanIndexManager.class, Log.class);
   private InfinispanBackendQueueProcessor remoteMaster;

   @Override
   protected WorkspaceHolder createWorkspaceHolder(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      //Don't use the BackendFactory here as we want to override it;
      //the standard BackendFactory will be created on-demand on the node if/when it's elected as master.
      remoteMaster = new InfinispanBackendQueueProcessor();
      remoteMaster.initialize(cfg, buildContext, this);
      return remoteMaster;
   }

   @Override
   protected DirectoryProvider createDirectoryProvider(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      //warn user we're overriding the configured DirectoryProvider - if anything different than Infinispan is selected.
      String directoryOption = cfg.getProperty("directory_provider", null);
      if (directoryOption != null && ! "infinispan".equals(directoryOption)) {
         log.ignoreDirectoryProviderProperty(indexName, directoryOption);
      }
      InfinispanDirectoryProvider infinispanDP = new InfinispanDirectoryProvider();
      infinispanDP.initialize(indexName, cfg, buildContext);
      return infinispanDP;
   }

   public boolean isMasterLocal() {
      return remoteMaster.isMasterLocal();
   }

}
