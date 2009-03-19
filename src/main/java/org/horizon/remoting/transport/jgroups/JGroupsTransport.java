package org.horizon.remoting.transport.jgroups;

import org.horizon.CacheException;
import org.horizon.commands.RPCCommand;
import org.horizon.config.GlobalConfiguration;
import org.horizon.config.parsing.XmlConfigHelper;
import org.horizon.lock.TimeoutException;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.marshall.Marshaller;
import org.horizon.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.horizon.remoting.InboundInvocationHandler;
import org.horizon.remoting.ReplicationException;
import org.horizon.remoting.ResponseFilter;
import org.horizon.remoting.ResponseMode;
import org.horizon.remoting.transport.Address;
import org.horizon.remoting.transport.DistributedSync;
import org.horizon.remoting.transport.Transport;
import org.horizon.statetransfer.StateTransferException;
import org.horizon.util.FileLookup;
import org.horizon.util.Util;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.ExtendedMembershipListener;
import org.jgroups.ExtendedMessageListener;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RspFilter;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An encapsulation of a JGroups transport
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class JGroupsTransport implements Transport, ExtendedMembershipListener, ExtendedMessageListener {
   public static final String CONFIGURATION_STRING = "configurationString";
   public static final String CONFIGURATION_XML = "configurationXml";
   public static final String CONFIGURATION_FILE = "configurationFile";
   private static final String DEFAULT_JGROUPS_CONFIGURATION_FILE = "flush-udp.xml";

   Channel channel;
   Address address;
   volatile List<Address> members = Collections.emptyList();
   volatile boolean coordinator = false;
   final Object membersListLock = new Object(); // guards members
   CommandAwareRpcDispatcher dispatcher;
   static final Log log = LogFactory.getLog(JGroupsTransport.class);
   static final boolean trace = log.isTraceEnabled();
   GlobalConfiguration c;
   Properties p;
   InboundInvocationHandler inboundInvocationHandler;
   Marshaller marshaller;
   ExecutorService asyncExecutor;
   CacheManagerNotifier notifier;
   final ConcurrentMap<String, StateTransferMonitor> stateTransfersInProgress = new ConcurrentHashMap<String, StateTransferMonitor>();
   private final FlushBasedDistributedSync flushTracker = new FlushBasedDistributedSync();
   volatile List<org.jgroups.Address> membersBlocked;
   AtomicBoolean flushInProgress = new AtomicBoolean(false);
   long distributedSyncTimeout;

   // ------------------------------------------------------------------------------------------------------------------
   // Lifecycle and setup stuff
   // ------------------------------------------------------------------------------------------------------------------

   public void initialize(GlobalConfiguration c, Properties p, Marshaller marshaller, ExecutorService asyncExecutor,
                          InboundInvocationHandler inboundInvocationHandler, CacheManagerNotifier notifier, long distributedSyncTimeout) {
      this.c = c;
      this.p = p;
      this.marshaller = marshaller;
      this.asyncExecutor = asyncExecutor;
      this.inboundInvocationHandler = inboundInvocationHandler;
      this.notifier = notifier;
      this.distributedSyncTimeout = distributedSyncTimeout;
   }

   public void start() {
      log.info("Starting JGroups Channel");

      initChannelAndRPCDispatcher();

      //otherwise just connect
      try {
         channel.connect(c.getClusterName());
      }
      catch (ChannelException e) {
         throw new CacheException("Unable to start JGroups Channel", e);
      }
      log.info("Cache local address is {0}", getAddress());
   }

   public void stop() {
      try {
         if (channel != null && channel.isOpen()) {
            log.info("Disconnecting and closing JGroups Channel");
            channel.disconnect();
            channel.close();
         }
      }
      catch (Exception toLog) {
         log.error("Problem closing channel; setting it to null", toLog);
      }

      channel = null;
      if (dispatcher != null) {
         log.info("Stopping the RpcDispatcher");
         dispatcher.stop();
      }

      if (members != null) members = null;
      coordinator = false;
      dispatcher = null;
   }

   private void initChannelAndRPCDispatcher() throws CacheException {
      buildChannel();
      // Channel.LOCAL *must* be set to false so we don't see our own messages - otherwise invalidations targeted at
      // remote instances will be received by self.
      channel.setOpt(Channel.LOCAL, false);
      channel.setOpt(Channel.AUTO_RECONNECT, true);
      channel.setOpt(Channel.AUTO_GETSTATE, false);
      channel.setOpt(Channel.BLOCK, true);
      dispatcher = new CommandAwareRpcDispatcher(channel, this,
                                                 asyncExecutor, inboundInvocationHandler, flushTracker, distributedSyncTimeout);
      MarshallerAdapter adapter = new MarshallerAdapter(marshaller);
      dispatcher.setRequestMarshaller(adapter);
      dispatcher.setResponseMarshaller(adapter);
   }

   private void buildChannel() {
      // TODO: Check for injected channels and for channel factories.
      // in order of preference - we first look for an external JGroups file, then a set of XML properties, and
      // finally the legacy JGroups String properties.
      String cfg;
      if (p != null) {
         if (p.containsKey(CONFIGURATION_FILE)) {
            cfg = p.getProperty(CONFIGURATION_FILE);
            try {
               channel = new JChannel(new FileLookup().lookupFileLocation(cfg));
            } catch (Exception e) {
               // move on to next method to configure the channel
               channel = null;
            }
         }

         if (channel == null && p.containsKey(CONFIGURATION_XML)) {
            cfg = p.getProperty(CONFIGURATION_XML);
            try {
               channel = new JChannel(XmlConfigHelper.stringToElement(cfg));
            } catch (Exception e) {
               // move on to next method to configure the channel
               channel = null;
            }
         }

         if (channel == null && p.containsKey(CONFIGURATION_STRING)) {
            cfg = p.getProperty(CONFIGURATION_STRING);
            try {
               channel = new JChannel(cfg);
            } catch (Exception e) {
               // move on to next method to configure the channel
               channel = null;
            }
         }
      }

      if (channel == null) {
         log.info("Unable to use any JGroups configuration mechanisms provided in properties {0}.  Using default JGroups configuration!", p);
         try {
            channel = new JChannel(new FileLookup().lookupFileLocation(DEFAULT_JGROUPS_CONFIGURATION_FILE));
         } catch (ChannelException e) {
            throw new CacheException("Unable to start JGroups channel", e);
         }
      }
   }

   // ------------------------------------------------------------------------------------------------------------------
   // querying cluster status
   // ------------------------------------------------------------------------------------------------------------------

   public boolean isCoordinator() {
      return coordinator;
   }

   public Address getCoordinator() {
      if (channel == null) return null;
      synchronized (membersListLock) {
         while (members == null || members.isEmpty()) {
            log.debug("Waiting on view being accepted");
            try {
               membersListLock.wait();
            } catch (InterruptedException e) {
               log.error("getCoordinator(): Interrupted while waiting for members to be set", e);
               break;
            }
         }
         return members == null || members.isEmpty() ? null : members.get(0);
      }
   }

   public List<Address> getMembers() {
      return members;
   }

   public boolean retrieveState(String cacheName, Address address, long timeout) throws StateTransferException {
      boolean cleanup = false;
      try {
         StateTransferMonitor mon = new StateTransferMonitor();
         if (stateTransfersInProgress.putIfAbsent(cacheName, mon) != null)
            throw new StateTransferException("There already appears to be a state transfer in progress for the cache named " + cacheName);

         cleanup = true;
         ((JChannel) channel).getState(toJGroupsAddress(address), cacheName, timeout, false);
         mon.waitForState();
         return mon.getSetStateException() == null;
      } catch (StateTransferException ste) {
         throw ste;
      } catch (Exception e) {
         if (log.isInfoEnabled()) log.info("Unable to retrieve state from member " + address, e);
         return false;
      } finally {
         if (cleanup) stateTransfersInProgress.remove(cacheName);
      }
   }

   public DistributedSync getDistributedSync() {
      return flushTracker;
   }

   public void blockRPC(Address... addresses) {
      if (flushInProgress.compareAndSet(false, true)) {
         // TODO make these configurable!!
         int retries = 5;
         int sleepBetweenRetries = 250;
         int sleepIncreaseFactor = 2;
         if (trace) log.trace("Attempting a partial flush on members {0} with up to {1} retries.", members, retries);

         boolean success = false;
         int i;
         for (i = 1; i <= retries; i++) {
            if (trace) log.trace("Attempt number " + i);
            try {

               if (addresses == null) {
                  success = channel.startFlush(false);
               } else {
                  membersBlocked = toJGroupsAddressList(addresses);
                  success = channel.startFlush(membersBlocked, false);
               }

               if (success) break;
               if (trace) log.trace("Channel.startFlush() returned false!");
            } catch (Exception e) {
               if (trace) log.trace("Caught exception attempting a partial flush", e);
            }
            try {
               if (trace)
                  log.trace("Partial state transfer failed.  Backing off for " + sleepBetweenRetries + " millis and retrying");
               Thread.sleep(sleepBetweenRetries);
               sleepBetweenRetries *= sleepIncreaseFactor;
            } catch (InterruptedException ie) {
               Thread.currentThread().interrupt();
            }
         }

         if (success) {
            if (log.isDebugEnabled()) log.debug("Partial flush between {0} succeeded!", membersBlocked);
         } else {
            flushInProgress.set(false);
            throw new CacheException("Could initiate partial flush between " + membersBlocked + "!");
         }
      } else {
         throw new CacheException("Cannot block RPC; a block is already in progress!");
      }
   }

   public void unblockRPC() {
      if (flushInProgress.get()) {
         try {
            if (membersBlocked == null) {
               channel.stopFlush();
            } else {
               channel.stopFlush(membersBlocked);
               membersBlocked = null;
            }
         } finally {
            flushInProgress.set(false);
         }
      }
   }

   public Address getAddress() {
      if (address == null && channel != null) {
         address = new JGroupsAddress(channel.getLocalAddress());
      }
      return address;
   }


   // ------------------------------------------------------------------------------------------------------------------
   // outbound RPC
   // ------------------------------------------------------------------------------------------------------------------

   public List<Object> invokeRemotely(List<Address> recipients, RPCCommand rpcCommand, ResponseMode mode, long timeout,
                                      boolean usePriorityQueue, ResponseFilter responseFilter, boolean supportReplay)
         throws Exception {

      if (recipients != null && recipients.isEmpty()) {
         // don't send if dest list is empty
         log.trace("Destination list is empty: no need to send message");
         return Collections.emptyList();
      }

      log.trace("dests={0}, command={1}, mode={2}, timeout={3}", recipients, rpcCommand, mode, timeout);

      // Acquire a "processing" lock so that any other code is made aware of a network call in progress
      // make sure this is non-exclusive since concurrent network calls are valid for most situations.
      flushTracker.acquireProcessingLock(false, distributedSyncTimeout, MILLISECONDS);
      boolean unlock = true;
      // if there is a FLUSH in progress, block till it completes
      flushTracker.blockUntilReleased(distributedSyncTimeout, MILLISECONDS);

      try {
         RspList rsps = dispatcher.invokeRemoteCommands(toJGroupsAddressVector(recipients), rpcCommand, toJGroupsMode(mode),
                                                        timeout, false, usePriorityQueue,
                                                        toJGroupsFilter(responseFilter), supportReplay);

         if (mode == ResponseMode.ASYNCHRONOUS) return Collections.emptyList();// async case

         if (trace)
            log.trace("Cache [{0}]: responses for command {1}:\n{2}", getAddress(), rpcCommand.getClass().getSimpleName(), rsps);

         // short-circuit no-return-value calls.
         if (rsps == null) return Collections.emptyList();
         List<Object> retval = new ArrayList<Object>(rsps.size());

         for (Rsp rsp : rsps.values()) {
            if (rsp.wasSuspected() || !rsp.wasReceived()) {
               CacheException ex;
               if (rsp.wasSuspected()) {
                  ex = new SuspectException("Suspected member: " + rsp.getSender());
               } else {
                  ex = new TimeoutException("Replication timeout for " + rsp.getSender());
               }
               retval.add(new ReplicationException("rsp=" + rsp, ex));
            } else {
               Object value = rsp.getValue();
               if (value instanceof Exception && !(value instanceof ReplicationException)) {
                  // if we have any application-level exceptions make sure we throw them!!
                  if (trace) log.trace("Recieved exception'" + value + "' from " + rsp.getSender());
                  throw (Exception) value;
               }
               retval.add(value);
            }
         }
         return retval;
      } finally {
         // release the "processing" lock so that other threads are aware of the network call having completed
         if (unlock) flushTracker.releaseProcessingLock();
      }
   }

   private int toJGroupsMode(ResponseMode mode) {
      switch (mode) {
         case ASYNCHRONOUS:
            return GroupRequest.GET_NONE;
         case SYNCHRONOUS:
            return GroupRequest.GET_ALL;
         case WAIT_FOR_VALID_RESPONSE:
            return GroupRequest.GET_MAJORITY;
      }
      throw new CacheException("Unknown response mode " + mode);
   }

   private RspFilter toJGroupsFilter(ResponseFilter responseFilter) {
      return responseFilter == null ? null : new JGroupsResponseFilterAdapter(responseFilter);
   }

   // ------------------------------------------------------------------------------------------------------------------
   // Implementations of JGroups interfaces
   // ------------------------------------------------------------------------------------------------------------------

   public void viewAccepted(View newView) {
      Vector<org.jgroups.Address> newMembers = newView.getMembers();
      if (log.isInfoEnabled()) log.info("Received new cluster view: {0}", newView);
      synchronized (membersListLock) {
         boolean needNotification = false;
         if (newMembers != null) {

            // TODO: Implement breaking stale locks for dead members.  This should be in the TxINterceptor or TransactionTable, with a listener on the cache manager.
//            if (members != null) {
            // we had a membership list before this event.  Check to make sure we haven't lost any members,
            // and if so, determine what members have been removed
            // and roll back any tx and break any locks
//               List<org.jgroups.Address> removed = toJGroupsAddressList(members);
//               removed.removeAll(newMembers);
//               spi.getInvocationContext().getOptionOverrides().setSkipCacheStatusCheck(true);
//                  if (root != null)
//                  {
            //removeLocksForDeadMembers(root.getDelegationTarget(), removed);
//                  }
//            }

            // we need a defensive copy anyway
            members = fromJGroupsAddressList(newMembers);
            needNotification = true;
         }
         // Now that we have a view, figure out if we are the coordinator
         coordinator = (members != null && members.size() != 0 && members.get(0).equals(getAddress()));

         // now notify listeners - *after* updating the coordinator. - JBCACHE-662
         if (needNotification && notifier != null) {
            notifier.notifyViewChange(members, getAddress());
         }

         // Wake up any threads that are waiting to know about who the coordinator is
         membersListLock.notifyAll();
      }
   }

   public void suspect(org.jgroups.Address suspected_mbr) {
      // no-op
   }

   public void block() {
      flushTracker.acquireSync();
   }

   public void unblock() {
      flushTracker.releaseSync();
   }

   public void receive(Message msg) {
      // no-op
   }

   public byte[] getState() {
      throw new UnsupportedOperationException("Retrieving state for the entire cache system is not supported!");
   }

   public void setState(byte[] state) {
      throw new UnsupportedOperationException("Setting state for the entire cache system is not supported!");
   }

   public byte[] getState(String state_id) {
      throw new UnsupportedOperationException("Non-stream-based state retrieval is not supported!  Make sure you use the JGroups STREAMING_STATE_TRANSFER protocol!");
   }

   public void setState(String state_id, byte[] state) {
      throw new UnsupportedOperationException("Non-stream-based state retrieval is not supported!  Make sure you use the JGroups STREAMING_STATE_TRANSFER protocol!");
   }

   public void getState(OutputStream ostream) {
      throw new UnsupportedOperationException("Retrieving state for the entire cache system is not supported!");
   }

   public void getState(String cacheName, OutputStream ostream) {
      if (trace)
         log.trace("Received request to generate state for cache named '{0}'.  Attempting to generate state.", cacheName);
      try {
         inboundInvocationHandler.generateState(cacheName, ostream);
      } catch (StateTransferException e) {
         log.error("Caught while responding to state transfer request", e);
      } finally {
         Util.flushAndCloseStream(ostream);
      }
   }

   public void setState(InputStream istream) {
      throw new UnsupportedOperationException("Setting state for the entire cache system is not supported!");
   }

   public void setState(String cacheName, InputStream istream) {
      StateTransferMonitor mon = null;
      try {
         if (trace) log.trace("Received state for cache named '{0}'.  Attempting to apply state.", cacheName);
         mon = stateTransfersInProgress.get(cacheName);
         inboundInvocationHandler.applyState(cacheName, istream);
         mon.notifyStateReceiptSucceeded();
      } catch (Exception e) {
         log.error("Failed setting state", e);
         mon.notifyStateReceiptFailed(e instanceof StateTransferException ? (StateTransferException) e : new StateTransferException(e));
      } finally {
         Util.closeStream(istream);
      }
   }


   // ------------------------------------------------------------------------------------------------------------------
   // Helpers to convert between Address types
   // ------------------------------------------------------------------------------------------------------------------

   private Vector<org.jgroups.Address> toJGroupsAddressVector(List<Address> list) {
      if (list == null) return null;
      if (list.isEmpty()) return new Vector<org.jgroups.Address>();

      Vector<org.jgroups.Address> retval = new Vector<org.jgroups.Address>(list.size());
      for (Address a : list) {
         JGroupsAddress ja = (JGroupsAddress) a;
         retval.add(ja.address);
      }
      return retval;
   }

   private List<org.jgroups.Address> toJGroupsAddressList(Address... addresses) {
      if (addresses == null) return null;
      if (addresses.length == 0) return Collections.emptyList();

      List<org.jgroups.Address> retval = new ArrayList<org.jgroups.Address>(addresses.length);
      for (Address a : addresses) {
         JGroupsAddress ja = (JGroupsAddress) a;
         retval.add(ja.address);
      }
      return retval;
   }

   private org.jgroups.Address toJGroupsAddress(Address a) {
      return ((JGroupsAddress) a).address;
   }

   private List<Address> fromJGroupsAddressList(List<org.jgroups.Address> list) {
      if (list == null || list.isEmpty()) return Collections.emptyList();

      List<Address> retval = new ArrayList<Address>(list.size());
      for (org.jgroups.Address a : list) {
         retval.add(new JGroupsAddress(a));
      }
      return retval;
   }
}
