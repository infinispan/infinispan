package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import static org.infinispan.commands.control.RehashControlCommand.Type.*;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import static org.infinispan.remoting.rpc.ResponseMode.SYNCHRONOUS;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
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
  
      log.debug("Starting join rehash[enabled={0}, joinComplete={1}] on node {2} ",
               conf.isRehashEnabled(), dmi.isJoinComplete(), getMyAddress());
      
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
         else {
            chNew = createConsistentHash(conf, chOld.getCaches(), dmi.topologyInfo, self);
         }

         dmi.setConsistentHash(chNew);
         try {
            if (conf.isRehashEnabled()) {
               // 3.  Enable TX logging
               transactionLogger.enable();
   
               // 4.  Broadcast new temp CH
               broadcastNewCh();
   
               // 6.  pull state from everyone.
               Address myAddress = rpcManager.getTransport().getAddress();
               
               RehashControlCommand cmd = cf.buildRehashControlCommand(PULL_STATE_JOIN, myAddress, null, chOld, chNew,null);
               // TODO I should be able to process state chunks from different nodes simultaneously!!
               
               List<Address> providers = chNew.getStateProvidersOnJoin(self, conf.getNumOwners());
               List<Response> resps = rpcManager.invokeRemotely(providers, cmd, SYNCHRONOUS, conf.getRehashRpcTimeout(), true);
   
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
               broadcastNewCh();
            }                                 
         } finally {
            // 10.
            rpcManager.broadcastRpcCommand(cf.buildRehashControlCommand(JOIN_REHASH_END, self), true, true);
            
            if (conf.isRehashEnabled()) {
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
            log.info("Completed join rehash on node {0} in {1}!", self, Util.prettyPrintTime(System.currentTimeMillis() - start));
         else
            log.info("Completed join rehash on node {0}!", self);
      }
   }

   private void broadcastNewCh() {
      RehashControlCommand rehashControlCommand = cf.buildRehashControlCommand(JOIN_REHASH_START, self);
      rehashControlCommand.setNodeTopologyInfo(dmi.topologyInfo.getNodeTopologyInfo(rpcManager.getAddress()));
      List<Response> responseList = rpcManager.invokeRemotely(null, rehashControlCommand, true, true);
      updateTopologyInfo(responseList);
   }

   private void updateTopologyInfo(List<Response> responseList) {
      for (Response r : responseList) {
         if(r instanceof SuccessfulResponse) {
            SuccessfulResponse sr = (SuccessfulResponse) r;
            NodeTopologyInfo nti = (NodeTopologyInfo) sr.getResponseValue();
            if (nti != null) {
               dmi.topologyInfo.addNodeTopologyInfo(nti.getAddress(), nti);
            }
         }
         else if(trace) {  // will ignore unsuccessful response
            log.trace("updateTopologyInfo will ignore unsuccessful response (another node may not be ready), got response with success=" + r.isSuccessful() +", is a " + r.getClass().getSimpleName());
         }
      }
      if (log.isTraceEnabled()) log.trace("Topology after after getting cluster info: " + dmi.topologyInfo);
   }

   private ConsistentHash retrieveOldCH(boolean trace) throws InterruptedException, IllegalAccessException,
                    InstantiationException, ClassNotFoundException {
        
        // this happens in a loop to ensure we receive the correct CH and not a "union".
        // TODO make at least *some* of these configurable!
        ConsistentHash result = null;
        long minSleepTime = 500, maxSleepTime = 2000; // sleep time between retries
        int maxWaitTime = (int) conf.getRehashRpcTimeout() * 10; // after which we give up!
        Random rand = new Random();
        long giveupTime = System.currentTimeMillis() + maxWaitTime;
        do {
            if (trace)
                log.trace("Requesting old consistent hash from coordinator");
            List<Response> resp;
            List<Address> addresses;
            try {
                resp = rpcManager.invokeRemotely(coordinator(), cf.buildRehashControlCommand(
                                JOIN_REQ, self), SYNCHRONOUS, conf.getRehashRpcTimeout(),
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
                result = createConsistentHash(conf, addresses, dmi.topologyInfo);
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

   public Address getMyAddress() {
      return rpcManager != null && rpcManager.getTransport() != null ? rpcManager.getTransport().getAddress() : null;
   }
}
