package org.infinispan.interceptors.impl;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * Base class for replication and distribution interceptors.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public abstract class ClusteringInterceptor extends BaseRpcInterceptor {

   protected CommandsFactory cf;
   protected EntryFactory entryFactory;
   protected LockManager lockManager;
   protected DataContainer dataContainer;
   protected StateTransferManager stateTransferManager;

   @Inject
   public void injectDependencies(CommandsFactory cf, EntryFactory entryFactory, LockManager lockManager,
         DataContainer dataContainer, StateTransferManager stateTransferManager) {
      this.cf = cf;
      this.entryFactory = entryFactory;
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.stateTransferManager = stateTransferManager;
   }
}
