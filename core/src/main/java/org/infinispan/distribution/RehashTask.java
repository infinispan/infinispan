package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;

/**
 * A task that handles the rehashing of data in the cache system wheh nodes join or leave the cluster.  This abstract
 * class contains common functionality.  Subclasses will specify different behavior for nodes joining and leaving.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class RehashTask implements Callable<Void> {
   protected DistributionManager distributionManager;
   protected RpcManager rpcManager;
   protected Configuration configuration;
   protected CommandsFactory cf;
   protected DataContainer dataContainer;
   protected final Address self;
   private final AtomicInteger counter = new AtomicInteger(0);
   protected final Log log = LogFactory.getLog(getClass());
   protected final boolean trace = log.isTraceEnabled();

   protected final ExecutorService statePullExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, "Rehasher-" + self + "-Worker-" + counter.getAndIncrement());
            th.setDaemon(true);
            return th;
         }
      });


   protected RehashTask(DistributionManagerImpl distributionManager, RpcManager rpcManager,
            Configuration configuration, CommandsFactory cf, DataContainer dataContainer) {
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.self = rpcManager.getAddress();
   }

   public Void call() throws Exception {
      distributionManager.setRehashInProgress(true);
      try {
         performRehash();
         return null;
      } finally {
         distributionManager.setRehashInProgress(false);
      }
   }

   protected abstract void performRehash() throws Exception;

   protected Collection<Address> coordinator() {
      return Collections.singleton(rpcManager.getTransport().getCoordinator());
   }


}
