package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import static org.infinispan.commands.control.RehashControlCommand.Type.*;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import static org.infinispan.distribution.ConsistentHashHelper.createConsistentHash;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 5.  JoinTask: This is a PULL based rehash.  JoinTask is kicked off on the JOINER.
 * <ul>
 * <li>5.1.  Obtain OLD_CH from coordinator (using GetConsistentHashCommand) <li>
 * <li>5.2.  Generate TEMP_CH (which is a union of OLD_CH and NEW_CH)</li>
 * <li>5.3.  Broadcast TEMP_CH across the cluster (using InstallConsistentHashCommand)</li>
 * <li>5.4.  Log all incoming writes/txs and respond with a positive ack.</li>
 * <li>5.5.  Ignore incoming reads, forcing callers to check next owner of data.</li>
 * <li>5.6.  Ping each node in OLD_CH's view and ask for state (PullStateCommand)</li>
 * <li>5.7.  Apply state received from 5.6.</li>
 * <li>5.8.  Drain tx log and apply, stop logging writes once drained.</li>
 * <li>5.9.  Reverse 5.5.</li>
 * <li>5.10. Broadcast NEW_CH so this is applied (using InstallConsistentHashCommand)</li>
 * <li>5.11. Loop through data container and unicast invalidations for keys that "could" exist on OLD_CH and not in NEW_CH</li>
 * <ul>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class JoinTask extends RehashTask {

   private static final Log log = LogFactory.getLog(JoinTask.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Address self;

   public JoinTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
            DataContainer dataContainer, DistributionManagerImpl dmi) {
      super(dmi, rpcManager, conf, commandsFactory, dataContainer);      
      this.self = rpcManager.getTransport().getAddress();
   }

   @SuppressWarnings("unchecked")
   private List<Address> parseResponses(List<Response> resp) {
      for (Response r : resp) {
         if (r instanceof SuccessfulResponse) {
            return (List<Address>) ((SuccessfulResponse) r).getResponseValue();
         }
      }
      return null;
   }

   @SuppressWarnings("unchecked")
   private Map<Object, InternalCacheValue> getStateFromResponse(SuccessfulResponse r) {
      return (Map<Object, InternalCacheValue>) r.getResponseValue();
   }

   protected void performRehash() throws Exception {
      long start = trace ? System.currentTimeMillis() : 0;
      if (log.isDebugEnabled()) log.debug("Commencing rehash on node: " + getMyAddress() + ". Before start, dmi.joinComplete = " + dmi.isJoinComplete());
      TransactionLogger transactionLogger = dmi.getTransactionLogger();
      boolean unlocked = false;
      ConsistentHash chOld;
      ConsistentHash chNew;
      try {
         if (dmi.isJoinComplete()) {
            throw new IllegalStateException("Join cannot be complete without rehash to finish (node " + getMyAddress() + " )");
         }
         // 1.  Get chOld from coord.         
         chOld = retrieveOldCH(trace);

         // 2.  new CH instance
         if (chOld.getCaches().contains(self))
            chNew = chOld;
         else
            chNew = createConsistentHash(configuration, chOld.getCaches(), self);
         
         dmi.setConsistentHash(chNew);
         try {
            if (configuration.isRehashEnabled()) {
               // 3.  Enable TX logging
               transactionLogger.enable();
   
               // 4.  Broadcast new temp CH
               rpcManager.broadcastRpcCommand(cf.buildRehashControlCommand(JOIN_REHASH_START, self), true, true);
   
               // 5.  txLogger being enabled will cause ClusteredGetCommands to return uncertain responses.
   
               // 6.  pull state from everyone.
               Address myAddress = rpcManager.getTransport().getAddress();
               
               RehashControlCommand cmd = cf.buildRehashControlCommand(PULL_STATE_JOIN, myAddress, null, chOld, chNew,null);
               // TODO I should be able to process state chunks from different nodes simultaneously!!
               List<Address> addressesWhoMaySendStuff = getAddressesWhoMaySendStuff(chNew, configuration.getNumOwners());
               List<Response> resps = rpcManager.invokeRemotely(addressesWhoMaySendStuff, cmd, SYNCHRONOUS, configuration.getRehashRpcTimeout(), true);
   
               // 7.  Apply state
               for (Response r : resps) {
                  if (r instanceof SuccessfulResponse) {
                     Map<Object, InternalCacheValue> state = getStateFromResponse((SuccessfulResponse) r);
                     dmi.applyState(chNew, state);
                  }
               }
   
               // 8.  Drain logs
               dmi.drainLocalTransactionLog();
               unlocked = true;
            } else {
               rpcManager.broadcastRpcCommand(cf.buildRehashControlCommand(JOIN_REHASH_START, self), true, true);
               if (trace) log.trace("Rehash not enabled, so not pulling state.");
            }                                 
         } finally {
            // 10.
            rpcManager.broadcastRpcCommand(cf.buildRehashControlCommand(JOIN_REHASH_END, self), true, true);
            
            if (configuration.isRehashEnabled()) {
               // 11.
               invalidateInvalidHolders(chOld, chNew);
            }
         }

      } catch (Exception e) {
         log.error("Caught exception!", e);
         throw new CacheException("Unexpected exception", e);
      } finally {
         if (!unlocked) transactionLogger.unlockAndDisable();
         dmi.setJoinComplete(true);
         if (trace)
            log.info("{0} completed join rehash in {1}!", self, Util.prettyPrintTime(System.currentTimeMillis() - start));
         else
            log.info("{0} completed join rehash!", self);
      }
   }

    private ConsistentHash retrieveOldCH(boolean trace) throws InterruptedException, IllegalAccessException,
                    InstantiationException, ClassNotFoundException {
        
        // this happens in a loop to ensure we receive the correct CH and not a "union".
        // TODO make at least *some* of these configurable!
        ConsistentHash result = null;
        long minSleepTime = 500, maxSleepTime = 2000; // sleep time between retries
        int maxWaitTime = (int) configuration.getRehashRpcTimeout() * 10; // after which we give up!
        Random rand = new Random();
        long giveupTime = System.currentTimeMillis() + maxWaitTime;
        do {
            if (trace)
                log.trace("Requesting old consistent hash from coordinator");
            List<Response> resp;
            List<Address> addresses;
            try {
                resp = rpcManager.invokeRemotely(coordinator(), cf.buildRehashControlCommand(
                                JOIN_REQ, self), SYNCHRONOUS, configuration.getRehashRpcTimeout(),
                                true);
                addresses = parseResponses(resp);
                if (log.isDebugEnabled())
                    log.debug("Retrieved old consistent hash address list {0}", addresses);
            } catch (TimeoutException te) {
                // timed out waiting for responses; retry!
                resp = null;
                addresses = null;
                if (log.isDebugEnabled())
                    log.debug("Timed out waiting for responses.");
            }

            if (addresses == null) {
                long time = rand.nextInt((int) (maxSleepTime - minSleepTime) / 10);
                time = (time * 10) + minSleepTime;
                if (trace)
                    log.trace("Sleeping for {0}", Util.prettyPrintTime(time));
                Thread.sleep(time); // sleep for a while and retry
            } else {
                result = createConsistentHash(configuration, addresses);
            }
        } while (result == null && System.currentTimeMillis() < giveupTime);

        if (result == null)
            throw new CacheException(
                            "Unable to retrieve old consistent hash from coordinator even after several attempts at sleeping and retrying!");
        return result;
    }

   @Override
   protected Log getLog() {
      return log;
   }

   /**
    * Retrieves a List of Address of who should be sending state to the joiner (self), given a repl count (numOwners)
    * for each entry.
    * <p />
    * The algorithm essentially works like this.  Given a list of all Addresses in the system (ordered by their positions
    * in the new consistent hash wheel), locate where the current address (self, the joiner) is, on this list.  Addresses
    * from (replCount - 1) positions behind self, and 1 position ahead of self would be sending state.
    * <p />
    * @param replCount
    * @return
    */
   List<Address> getAddressesWhoMaySendStuff(ConsistentHash ch, int replCount) {
      List<Address> l = new LinkedList<Address>();
      List<Address> caches = ch.getCaches();
      int selfIdx = caches.indexOf(self);
      if (selfIdx >= replCount - 1) {
         l.addAll(caches.subList(selfIdx - replCount + 1, selfIdx));
      } else {
         l.addAll(caches.subList(0, selfIdx));
         int alreadyCollected = l.size();
         l.addAll(caches.subList(caches.size() - replCount + 1 + alreadyCollected, caches.size()));
      }

      Address plusOne;
      if (selfIdx == caches.size() - 1)
         plusOne = caches.get(0);
      else
         plusOne = caches.get(selfIdx + 1);

      if (!l.contains(plusOne)) l.add(plusOne);
      return l;
   }

   public Address getMyAddress() {
      return rpcManager != null && rpcManager.getTransport() != null ? rpcManager.getTransport().getAddress() : null;
   }
}
