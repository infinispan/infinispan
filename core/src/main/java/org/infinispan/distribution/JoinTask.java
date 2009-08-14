package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.PullStateCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
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
import java.util.concurrent.Future;

/**
 * 5.  JoinTask: This is a PULL based rehash.  JoinTask is kicked off on the JOINER. 5.1.  Obtain OLD_CH from
 * coordinator (using GetConsistentHashCommand) 5.2.  Generate TEMP_CH (which is a union of OLD_CH and NEW_CH) 5.3.
 * Broadcast TEMP_CH across the cluster (using InstallConsistentHashCommand) 5.4.  Log all incoming writes/txs and
 * respond with a positive ack. 5.5.  Ignore incoming reads, forcing callers to check next owner of data. 5.6.  Ping
 * each node in OLD_CH's view and ask for state (PullStateCommand) 5.7.  Apply state received from 5.6. 5.8.  Drain tx
 * log and apply, stop logging writes once drained. 5.9.  Reverse 5.5. 5.10. Broadcast NEW_CH so this is applied (using
 * InstallConsistentHashCommand) 5.11. Loop through data container and unicast invalidations for keys that "could" exist
 * on OLD_CH and not in NEW_CH
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class JoinTask extends RehashTask {

   private static final Log log = LogFactory.getLog(JoinTask.class);
   ConsistentHash chOld;
   ConsistentHash chNew;
   //   ConsistentHash chTemp;
   CommandsFactory commandsFactory;
   TransactionLogger transactionLogger;
   DataContainer dataContainer;
   InterceptorChain interceptorChain;
   InvocationContextContainer icc;
   Address self;

   public JoinTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
                   TransactionLogger transactionLogger, DataContainer dataContainer, InterceptorChain interceptorChain,
                   InvocationContextContainer icc, DistributionManagerImpl dmi) {
      super(dmi, rpcManager, conf);
      this.commandsFactory = commandsFactory;
      this.transactionLogger = transactionLogger;
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      self = rpcManager.getTransport().getAddress();
   }

   protected void performRehash() throws Exception {
      log.trace("Starting rehash on new joiner");
      boolean unlocked = false;
      try {
         dmi.joinComplete = false;
         // 1.  Get chOld from coord.
         // this happens in a loop to ensure we receive the correct CH and not a "union".
         // TODO make at least *some* of these configurable!
         long sleepTime = 500; // sleep time between retries
         int incrementFactor = 2; // factor by wich to increment retry sleep
         int maxSleepTime = 600000; // after which we give up!

         do {
            log.trace("Requesting old consistent hash from coordinator");
            List<Response> resp = rpcManager.invokeRemotely(coordinator(), commandsFactory.buildGetConsistentHashCommand(self),
                                                            ResponseMode.SYNCHRONOUS, 100000, true);
            for (Response r : resp) {
               if (r instanceof SuccessfulResponse) {
                  List<Address> list = (List<Address>) ((SuccessfulResponse) r).getResponseValue();
                  chOld = createConsistentHash(list);
                  break;
               }
            }

            log.trace("Retrieved old consistent hash {0}", chOld);
            if (chOld == null) {
               if (sleepTime > maxSleepTime)
                  throw new CacheException("Unable to retrieve old consistent hash from coordinator even after several attempts at sleeping and retrying!");
               log.debug("Sleeping for {0}", Util.prettyPrintTime(sleepTime));
               Thread.sleep(sleepTime); // sleep for a while and retry
               sleepTime *= incrementFactor;
            }
         } while (chOld == null);

         // 2.  new CH instance
         chNew = createConsistentHash(chOld.getCaches(), self);
         dmi.setConsistentHash(chNew);

         // 3.  Enable TX logging
         transactionLogger.enable();

         // 4.  Broadcast new temp CH
         rpcManager.broadcastRpcCommand(commandsFactory.buildInstallConsistentHashCommand(self, true), true, true);

         // 5.  txLogger being enabled will cause CLusteredGetCommands to return uncertain responses.

         // 6.  pull state from everyone.
         Address myAddress = rpcManager.getTransport().getAddress();
         PullStateCommand cmd = commandsFactory.buildPullStateCommand(myAddress, chNew);
         // TODO I should be able to process state chunks from different nodes simultaneously!!
         // TODO I should only send this pull state request to nodes which I know will send me state.  Not everyone in chOld!!
         List<Response> resps = rpcManager.invokeRemotely(chOld.getCaches(), cmd, ResponseMode.SYNCHRONOUS, 10000, true);

         // 7.  Apply state
         for (Response r : resps) {
            if (r instanceof SuccessfulResponse) {
               Map<Object, InternalCacheValue> state = (Map<Object, InternalCacheValue>) ((SuccessfulResponse) r).getResponseValue();
               for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
                  if (chNew.locate(e.getKey(), configuration.getNumOwners()).contains(myAddress)) {
                     InternalCacheValue v = e.getValue();
                     PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle());
                     InvocationContext ctx = icc.createInvocationContext();
                     ctx.setFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_REMOTE_LOOKUP);
                     interceptorChain.invoke(ctx, put);
                  }
               }
            }
         }

         // 8.  Drain logs

         List<WriteCommand> c = null;
         while (transactionLogger.size() > 10) {
            c = transactionLogger.drain();
            apply(c);
         }

         c = transactionLogger.drainAndLock();
         apply(c);

         unlocked = true;
         // 9.
         transactionLogger.unlockAndDisable();

         // 10.
         // TODO this phase should also "tell" the coord that the join is complete so that other waiting joiners
         // may proceed.  Ideally another command, directed to the coord.
         rpcManager.broadcastRpcCommand(commandsFactory.buildInstallConsistentHashCommand(self, false), true, true);
         rpcManager.invokeRemotely(coordinator(), commandsFactory.buildJoinCompleteCommand(self), ResponseMode.SYNCHRONOUS,
                                   100000, true);

         // 11.
         Map<Address, Set<Object>> invalidations = new HashMap<Address, Set<Object>>();
         for (Object key : dataContainer.keySet()) {
            Collection<Address> invalidHolders = getInvalidHolders(key, chOld, chNew);
            for (Address a : invalidHolders) {
               Set<Object> s = invalidations.get(a);
               if (s == null) {
                  s = new HashSet<Object>();
                  invalidations.put(a, s);
               }
               s.add(key);
            }
         }

         Set<Future> futures = new HashSet<Future>();

         for (Map.Entry<Address, Set<Object>> e : invalidations.entrySet()) {
            InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(e.getValue().toArray());
            NotifyingNotifiableFuture f = new NotifyingFutureImpl(null);
            rpcManager.invokeRemotelyInFuture(Collections.singletonList(e.getKey()), ic, true, f);
            futures.add(f);
         }

         for (Future f : futures) f.get();
      } catch (Exception e) {
         log.warn("Caught error performing rehash!", e);
      } finally {
         if (!unlocked) transactionLogger.unlockAndDisable();
         dmi.joinComplete = true;
      }
   }

   private Collection<Address> getInvalidHolders(Object key, ConsistentHash chOld, ConsistentHash chNew) {
      List<Address> oldOwners = chOld.locate(key, configuration.getNumOwners());
      List<Address> newOwners = chNew.locate(key, configuration.getNumOwners());

      List<Address> toInvalidate = new LinkedList<Address>(oldOwners);
      toInvalidate.removeAll(newOwners);

      return toInvalidate;
   }

   private void apply(List<WriteCommand> c) {
      for (WriteCommand cmd : c) {
         InvocationContext ctx = icc.createInvocationContext();
         ctx.setFlags(Flag.SKIP_REMOTE_LOOKUP);
         interceptorChain.invoke(ctx, cmd);
      }
   }
}
