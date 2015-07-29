package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.locks.LockManager;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Base class for replication and distribution interceptors.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public abstract class ClusteringInterceptor extends BaseRpcInterceptor {

   protected CommandsFactory cf;
   protected EntryFactory entryFactory;
   protected LockManager lockManager;
   protected DataContainer dataContainer;
   protected StateTransferManager stateTransferManager;

   @Inject
   public void injectDependencies(CommandsFactory cf, EntryFactory entryFactory,
                                  LockManager lockManager, DataContainer dataContainer,
                                  StateTransferManager stateTransferManager) {
      this.cf = cf;
      this.entryFactory = entryFactory;
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.stateTransferManager = stateTransferManager;
   }
}
