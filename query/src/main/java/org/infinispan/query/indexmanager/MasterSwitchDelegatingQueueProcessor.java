/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.indexmanager;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;

import org.hibernate.search.backend.IndexingMonitor;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.backend.spi.BackendQueueProcessor;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.hibernate.search.spi.WorkerBuildContext;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Switches between local indexing by delegating to a traditional BackendQueueProcessor
 * or to the custom InfinispanCommandsBackend to delegate to remote nodes.
 *
 * @author Sanne Grinovero
 */
public class MasterSwitchDelegatingQueueProcessor implements BackendQueueProcessor {

   private static final Log log = LogFactory.getLog(MasterSwitchDelegatingQueueProcessor.class, Log.class);

   private final BackendQueueProcessor localMaster;
   private final InfinispanCommandsBackend remoteMaster;

   public MasterSwitchDelegatingQueueProcessor(BackendQueueProcessor localMaster, InfinispanCommandsBackend remoteMaster) {
      this.localMaster = localMaster;
      this.remoteMaster = remoteMaster;
   }

   @Override
   public void initialize(Properties props, WorkerBuildContext context, DirectoryBasedIndexManager indexManager) {
      localMaster.initialize(props, context, indexManager);
      remoteMaster.initialize(props, context, indexManager);
   }

   @Override
   public void close() {
      remoteMaster.close();
      localMaster.close();
   }

   @Override
   public void applyWork(List<LuceneWork> workList, IndexingMonitor monitor) {
      if (remoteMaster.isMasterLocal()) {
         log.applyingChangeListLocally(workList);
         localMaster.applyWork(workList, monitor);
      }
      else {
         log.applyingChangeListRemotely(workList);
         remoteMaster.applyWork(workList, monitor);
      }
   }

   @Override
   public void applyStreamWork(LuceneWork singleOperation, IndexingMonitor monitor) {
      if (remoteMaster.isMasterLocal()) {
         localMaster.applyStreamWork(singleOperation, monitor);
      }
      else {
         remoteMaster.applyStreamWork(singleOperation, monitor);
      }
   }

   @Override
   public Lock getExclusiveWriteLock() {
      //This is non-sense for the remote delegator, so we only acquire it on the local master queue.
      //The lock is currently used only by org.hibernate.search.store.impl.FSMasterDirectoryProvider
      //which should not be used with this backend anyway unless in very exotic architectures.
      //TODO: even the FSMasterDirectoryProvider could be rewritten to not need such a lock.
      return localMaster.getExclusiveWriteLock();
   }

   @Override
   public void indexMappingChanged() {
      //Needs to notify all backends, so they can all reconfigure themselves if needed.
      remoteMaster.indexMappingChanged();
      localMaster.indexMappingChanged();
   }

}
