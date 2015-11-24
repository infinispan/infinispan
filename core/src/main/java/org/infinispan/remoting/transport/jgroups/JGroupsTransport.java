package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.AbstractTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.Channel;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.View;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RspFilter;
import org.jgroups.blocks.mux.Muxer;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.tom.TOA;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.TopologyUUID;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.factories.KnownComponentNames.GLOBAL_MARSHALLER;

/**
 * An encapsulation of a JGroups transport. JGroups transports can be configured using a variety of
 * methods, usually by passing in one of the following properties:
 * <ul>
 * <li><tt>configurationString</tt> - a JGroups configuration String</li>
 * <li><tt>configurationXml</tt> - JGroups configuration XML as a String</li>
 * <li><tt>configurationFile</tt> - String pointing to a JGroups XML configuration file</li>
 * <li><tt>channelLookup</tt> - Fully qualified class name of a
 * {@link org.infinispan.remoting.transport.jgroups.JGroupsChannelLookup} instance</li>
 * </ul>
 * These are normally passed in as Properties in
 * {@link org.infinispan.configuration.global.TransportConfigurationBuilder#withProperties(java.util.Properties)} or
 * in the Infinispan XML configuration file.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JGroupsTransport extends AbstractTransport implements MembershipListener {
   public static final String CONFIGURATION_STRING = "configurationString";
   public static final String CONFIGURATION_XML = "configurationXml";
   public static final String CONFIGURATION_FILE = "configurationFile";
   public static final String CHANNEL_LOOKUP = "channelLookup";
   protected static final String DEFAULT_JGROUPS_CONFIGURATION_FILE = "default-configs/default-jgroups-udp.xml";

   static final Log log = LogFactory.getLog(JGroupsTransport.class);
   static final boolean trace = log.isTraceEnabled();

   protected boolean connectChannel = true, disconnectChannel = true, closeChannel = true;
   protected CommandAwareRpcDispatcher dispatcher;
   protected TypedProperties props;
   protected StreamingMarshaller marshaller;
   protected CacheManagerNotifier notifier;
   protected GlobalComponentRegistry gcr;
   private TimeService timeService;
   protected InboundInvocationHandler globalHandler;
   private ScheduledExecutorService timeoutExecutor;

   private boolean globalStatsEnabled;
   private MBeanServer mbeanServer;
   private String domain;

   protected Channel channel;
   protected Address address;
   protected Address physicalAddress;

   // these members are not valid until we have received the first view on a second thread
   // and channelConnectedLatch is signaled
   protected volatile int viewId = -1;
   protected volatile List<Address> members = null;
   protected volatile Address coordinator = null;
   protected volatile boolean isCoordinator = false;
   protected Lock viewUpdateLock = new ReentrantLock();
   protected Condition viewUpdateCondition = viewUpdateLock.newCondition();

   /**
    * This form is used when the transport is created by an external source and passed in to the
    * GlobalConfiguration.
    *
    * @param channel
    *           created and running channel to use
    */
   public JGroupsTransport(Channel channel) {
      this.channel = channel;
      if (channel == null)
         throw new IllegalArgumentException("Cannot deal with a null channel!");
      if (channel.isConnected())
         throw new IllegalArgumentException("Channel passed in cannot already be connected!");
   }

   public JGroupsTransport() {
   }

   @Override
   public Log getLog() {
      return log;
   }

   protected ScheduledExecutorService getTimeoutExecutor() {
      return timeoutExecutor;
  }

   // ------------------------------------------------------------------------------------------------------------------
   // Lifecycle and setup stuff
   // ------------------------------------------------------------------------------------------------------------------

   /**
    * Initializes the transport with global cache configuration and transport-specific properties.
    *
    * @param marshaller    marshaller to use for marshalling and unmarshalling
    * @param notifier      notifier to use
    * @param gcr           the global component registry
    */
   @Inject
   public void initialize(@ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller marshaller,
                          CacheManagerNotifier notifier, GlobalComponentRegistry gcr,
                          TimeService timeService, InboundInvocationHandler globalHandler,
                          @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.marshaller = marshaller;
      this.notifier = notifier;
      this.gcr = gcr;
      this.timeService = timeService;
      this.globalHandler = globalHandler;
      this.timeoutExecutor = timeoutExecutor;
   }

   @Override
   public void start() {
      props = TypedProperties.toTypedProperties(configuration.transport().properties());

      if (log.isInfoEnabled())
         log.startingJGroupsChannel(configuration.transport().clusterName());

      initChannelAndRPCDispatcher();
      startJGroupsChannelIfNeeded();

      waitForChannelToConnect();
   }

   protected void startJGroupsChannelIfNeeded() {
      String clusterName = configuration.transport().clusterName();
      if (connectChannel) {
         try {
            channel.connect(clusterName);
         } catch (Exception e) {
            throw new CacheException("Unable to start JGroups Channel", e);
         }

         try {
            // Normally this would be done by CacheManagerJmxRegistration but
            // the channel is not started when the cache manager starts but
            // when first cache starts, so it's safer to do it here.
            globalStatsEnabled = configuration.globalJmxStatistics().enabled();
            if (globalStatsEnabled) {
               String groupName = String.format("type=channel,cluster=%s", ObjectName.quote(clusterName));
               mbeanServer = JmxUtil.lookupMBeanServer(configuration);
               domain = JmxUtil.buildJmxDomain(configuration, mbeanServer, groupName);
               JmxConfigurator.registerChannel((JChannel) channel, mbeanServer, domain, clusterName, true);
            }
         } catch (Exception e) {
            throw new CacheException("Channel connected, but unable to register MBeans", e);
         }
      }
      address = fromJGroupsAddress(channel.getAddress());
      if (!connectChannel) {
         // the channel was already started externally, we need to initialize our member list
         viewAccepted(channel.getView());
      }
      if (log.isInfoEnabled())
         log.localAndPhysicalAddress(clusterName, getAddress(), getPhysicalAddresses());
   }

   @Override
   public int getViewId() {
      if (channel == null)
         throw new CacheException("The cache has been stopped and invocations are not allowed!");
      return viewId;
   }

   @Override
   public void waitForView(int viewId) throws InterruptedException {
      if (channel == null)
         return;
      log.tracef("Waiting on view %d being accepted", viewId);
      viewUpdateLock.lock();
      try {
         while (channel != null && getViewId() < viewId) {
            viewUpdateCondition.await();
         }
      } finally {
         viewUpdateLock.unlock();
      }
   }

   @Override
   public void stop() {
      String clusterName = configuration.transport().clusterName();
      try {
         if (disconnectChannel && channel != null && channel.isConnected()) {
            log.disconnectJGroups(clusterName);

            // Unregistering before disconnecting/closing because
            // after that the cluster name is null
            if (globalStatsEnabled) {
               JmxConfigurator.unregisterChannel((JChannel) channel, mbeanServer, domain, channel.getClusterName());
            }

            channel.disconnect();
         }
         if (closeChannel && channel != null && channel.isOpen()) {
            channel.close();
         }
      } catch (Exception toLog) {
         log.problemClosingChannel(toLog, clusterName);
      }

      if (dispatcher != null) {
         log.stoppingRpcDispatcher(clusterName);
         dispatcher.stop();
         if (channel != null) {
            // Remove reference to up_handler
            UpHandler handler = channel.getUpHandler();
            if (handler instanceof Muxer<?>) {
               @SuppressWarnings("unchecked")
               Muxer<UpHandler> mux = (Muxer<UpHandler>) handler;
               mux.setDefaultHandler(null);
            } else {
               channel.setUpHandler(null);
            }
         }
      }

      channel = null;
      viewId = -1;
      members = InfinispanCollections.emptyList();
      coordinator = null;
      isCoordinator = false;
      dispatcher = null;

      // Wake up any view waiters
      viewUpdateLock.lock();
      try {
         viewUpdateCondition.signalAll();
      } finally {
         viewUpdateLock.unlock();
      }

   }

   protected void initChannel() {
      final TransportConfiguration transportCfg = configuration.transport();
      if (channel == null) {
         buildChannel();
         String transportNodeName = transportCfg.nodeName();
         if (transportNodeName != null && transportNodeName.length() > 0) {
            long range = Short.MAX_VALUE * 2;
            long randomInRange = (long) ((Math.random() * range) % range) + 1;
            transportNodeName = transportNodeName + "-" + randomInRange;
            channel.setName(transportNodeName);
         }
      }

      // Channel.LOCAL *must* be set to false so we don't see our own messages - otherwise
      // invalidations targeted at remote instances will be received by self.
      // NOTE: total order needs to deliver own messages. the invokeRemotely method has a total order boolean
      //       that when it is false, it discard our own messages, maintaining the property needed
      channel.setDiscardOwnMessages(false);

      // if we have a TopologyAwareConsistentHash, we need to set our own address generator in JGroups
      if (transportCfg.hasTopologyInfo()) {
         // We can do this only if the channel hasn't been started already
         if (connectChannel) {
            ((JChannel) channel).setAddressGenerator(new AddressGenerator() {
               @Override
               public org.jgroups.Address generateAddress() {
                  return TopologyUUID.randomUUID(channel.getName(), transportCfg.siteId(),
                        transportCfg.rackId(), transportCfg.machineId());
               }
            });
         } else {
            if (channel.getAddress() instanceof TopologyUUID) {
               TopologyUUID topologyAddress = (TopologyUUID) channel.getAddress();
               if (!transportCfg.siteId().equals(topologyAddress.getSiteId())
                     || !transportCfg.rackId().equals(topologyAddress.getRackId())
                     || !transportCfg.machineId().equals(topologyAddress.getMachineId())) {
                  throw new CacheException("Topology information does not match the one set by the provided JGroups channel");
               }
            } else {
               throw new CacheException("JGroups address does not contain topology coordinates");
            }
         }
      }
   }

   private void initChannelAndRPCDispatcher() throws CacheException {
      initChannel();
      initRPCDispatcher();
   }

   protected void initRPCDispatcher() {
      dispatcher = new CommandAwareRpcDispatcher(channel, this, globalHandler, timeoutExecutor);
      MarshallerAdapter adapter = new MarshallerAdapter(marshaller);
      dispatcher.setRequestMarshaller(adapter);
      dispatcher.setResponseMarshaller(adapter);
      dispatcher.start();
   }

   // This is per CM, so the CL in use should be the CM CL
   private void buildChannel() {
	  FileLookup fileLookup = FileLookupFactory.newInstance();

      // in order of preference - we first look for an external JGroups file, then a set of XML
      // properties, and
      // finally the legacy JGroups String properties.
      String cfg;
      if (props != null) {
         if (props.containsKey(CHANNEL_LOOKUP)) {
            String channelLookupClassName = props.getProperty(CHANNEL_LOOKUP);

            try {
               JGroupsChannelLookup lookup = Util.getInstance(channelLookupClassName, configuration.classLoader());
               channel = lookup.getJGroupsChannel(props);
               connectChannel = lookup.shouldConnect();
               disconnectChannel = lookup.shouldDisconnect();
               closeChannel = lookup.shouldClose();
            } catch (ClassCastException e) {
               log.wrongTypeForJGroupsChannelLookup(channelLookupClassName, e);
               throw new CacheException(e);
            } catch (Exception e) {
               log.errorInstantiatingJGroupsChannelLookup(channelLookupClassName, e);
               throw new CacheException(e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_FILE)) {
            cfg = props.getProperty(CONFIGURATION_FILE);
            Collection<URL> confs = Collections.emptyList();
            try {
               confs = fileLookup.lookupFileLocations(cfg, configuration.classLoader());
            } catch (IOException io) {
               //ignore, we check confs later for various states
            }
            if (confs.isEmpty()) {
               throw log.jgroupsConfigurationNotFound(cfg);
            } else if (confs.size() > 1) {
               log.ambiguousConfigurationFiles(Util.toStr(confs));
            }
            try {
               channel = new JChannel(confs.iterator().next());
            } catch (Exception e) {
               throw log.errorCreatingChannelFromConfigFile(cfg, e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_XML)) {
            cfg = props.getProperty(CONFIGURATION_XML);
            try {
               channel = new JChannel(XmlConfigHelper.stringToElement(cfg));
            } catch (Exception e) {
               throw log.errorCreatingChannelFromXML(cfg, e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_STRING)) {
            cfg = props.getProperty(CONFIGURATION_STRING);
            try {
               channel = new JChannel(cfg);
            } catch (Exception e) {
               throw log.errorCreatingChannelFromConfigString(cfg, e);

            }
         }
      }

      if (channel == null) {
         log.unableToUseJGroupsPropertiesProvided(props);
         try {
            channel = new JChannel(fileLookup.lookupFileLocation(DEFAULT_JGROUPS_CONFIGURATION_FILE, configuration.classLoader()));
         } catch (Exception e) {
            throw log.errorCreatingChannelFromConfigFile(DEFAULT_JGROUPS_CONFIGURATION_FILE, e);
         }
      }
   }

   // ------------------------------------------------------------------------------------------------------------------
   // querying cluster status
   // ------------------------------------------------------------------------------------------------------------------

   @Override
   public boolean isCoordinator() {
      return isCoordinator;
   }

   @Override
   public Address getCoordinator() {
      return coordinator;
   }

   public void waitForChannelToConnect() {
      try {
         waitForView(0);
      } catch (InterruptedException e) {
         // The start method can't throw checked exceptions
         log.interruptedWaitingForCoordinator(e);
         Thread.currentThread().interrupt();
      }
   }

   @Override
   public List<Address> getMembers() {
      return members != null ? members : InfinispanCollections.<Address>emptyList();
   }

   @Override
   public boolean isMulticastCapable() {
      return channel.getProtocolStack().getTransport().supportsMulticasting();
   }

   @Override
   public Address getAddress() {
      if (address == null && channel != null) {
         address = fromJGroupsAddress(channel.getAddress());
      }
      return address;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      if (physicalAddress == null && channel != null) {
         org.jgroups.Address addr = (org.jgroups.Address) channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, channel.getAddress()));
         if (addr == null) {
            return InfinispanCollections.emptyList();
         }
         physicalAddress = new JGroupsAddress(addr);
      }
      return Collections.singletonList(physicalAddress);
   }

   // ------------------------------------------------------------------------------------------------------------------
   // outbound RPC
   // ------------------------------------------------------------------------------------------------------------------

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, ResponseFilter responseFilter,
                                                DeliverOrder deliverOrder, boolean anycast) throws Exception {
      CompletableFuture<Map<Address, Response>> future = invokeRemotelyAsync(recipients, rpcCommand, mode,
            timeout, responseFilter, deliverOrder, anycast);
      try {
         //no need to set a timeout for the future. The rpc invocation is guaranteed to complete within the timeout milliseconds
         return CompletableFutures.await(future);
      } catch (ExecutionException e) {
         throw Util.rewrapAsCacheException(e.getCause());
      }
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpcCommand,
                                                                        ResponseMode mode, long timeout,
                                                                        ResponseFilter responseFilter,
                                                                        DeliverOrder deliverOrder,
                                                                        boolean anycast) throws Exception {
      if (recipients != null && recipients.isEmpty()) {
         // don't send if recipients list is empty
         log.trace("Destination list is empty: no need to send message");
         return CompletableFuture.completedFuture(InfinispanCollections.emptyMap());
      }
      boolean totalOrder = deliverOrder == DeliverOrder.TOTAL;

      if (trace)
         log.tracef("dests=%s, command=%s, mode=%s, timeout=%s", recipients, rpcCommand, mode, timeout);
      Address self = getAddress();
      boolean ignoreLeavers = mode == ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS || mode == ResponseMode.WAIT_FOR_VALID_RESPONSE;
      if (mode.isSynchronous() && recipients != null && !getMembers().containsAll(recipients)) {
         if (!ignoreLeavers) { // SYNCHRONOUS
            CompletableFuture<Map<Address, Response>> future = new CompletableFuture<>();
            future.completeExceptionally(new SuspectException("One or more nodes have left the cluster while replicating command " + rpcCommand));
            return future;
         }
      }

      List<org.jgroups.Address> jgAddressList = toJGroupsAddressListExcludingSelf(recipients, totalOrder);
      List<Address> localMembers = this.members;
      int membersSize = localMembers.size();
      boolean broadcast = jgAddressList == null || recipients.size() == membersSize;
      if (membersSize < 3 || (jgAddressList != null && jgAddressList.size() < 2)) broadcast = false;
      CompletableFuture<RspList<Response>> rspListFuture = null;
      SingleResponseFuture singleResponseFuture = null;
      org.jgroups.Address singleJGAddress = null;

      if (broadcast) {
         rspListFuture = dispatcher.invokeRemoteCommands(null, rpcCommand, toJGroupsMode(mode), timeout,
               toJGroupsFilter(responseFilter), deliverOrder);
      } else if (totalOrder) {
         rspListFuture = dispatcher.invokeRemoteCommands(jgAddressList, rpcCommand, toJGroupsMode(mode),
               timeout, toJGroupsFilter(responseFilter), deliverOrder);
      } else {
         if (jgAddressList == null || !jgAddressList.isEmpty()) {
            boolean singleRecipient = false;
            boolean skipRpc = false;
            if (jgAddressList == null) {
               if (membersSize == 1) {
                  skipRpc = true;
               } else if (!ignoreLeavers && membersSize == 2) {
                  singleRecipient = true;
                  if (localMembers.get(0).equals(self)) {
                     singleJGAddress = toJGroupsAddress(localMembers.get(1));
                  } else {
                     singleJGAddress = toJGroupsAddress(localMembers.get(0));
                  }
               }
            } else if (jgAddressList.size() == 1 && !ignoreLeavers) {
               singleRecipient = true;
               singleJGAddress = jgAddressList.get(0);
            }
            if (skipRpc) {
               return CompletableFutures.returnEmptyMap();
            }

            if (singleRecipient) {
               if (singleJGAddress == null) singleJGAddress = jgAddressList.get(0);
               singleResponseFuture = dispatcher.invokeRemoteCommand(singleJGAddress, rpcCommand,
                     toJGroupsMode(mode), timeout, deliverOrder);
            } else {
               rspListFuture = dispatcher.invokeRemoteCommands(jgAddressList, rpcCommand, toJGroupsMode(
                     mode), timeout, toJGroupsFilter(responseFilter), deliverOrder);

            }
         } else {
            return CompletableFutures.returnEmptyMap();
         }
      }

      if (mode.isAsynchronous()) {
         return CompletableFutures.returnEmptyMap();
      }

      if (singleResponseFuture != null) {
         // Unicast request
         return singleResponseFuture.thenApply(rsp -> {
            if (trace) log.tracef("Response: %s", rsp);
            Address sender = fromJGroupsAddress(rsp.getSender());
            Response response = checkRsp(rsp, sender, responseFilter != null, ignoreLeavers);
            return Collections.singletonMap(sender, response);
         });
      } else if (rspListFuture != null) {
         // Broadcast/anycast request
         return rspListFuture.thenApply(rsps -> {
            if (trace) log.tracef("Responses: %s", rsps);
            Map<Address, Response> retval = new HashMap<>(rsps.size());
            boolean hasResponses = false;
            boolean hasValidResponses = false;
            for (Rsp<Response> rsp : rsps.values()) {
               hasResponses |= rsp.wasReceived();
               Address sender = fromJGroupsAddress(rsp.getSender());
               Response response = checkRsp(rsp, sender, responseFilter != null, ignoreLeavers);
               if (response != null) {
                  hasValidResponses = true;
                  retval.put(sender, response);
               }
            }

            if (!hasValidResponses) {
               // PartitionHandlingInterceptor relies on receiving a RpcException if there are only invalid responses
               // But we still need to throw a TimeoutException if there are no responses at all.
               if (hasResponses) {
                  throw new RpcException(String.format("Received invalid responses from all of %s", recipients));
               } else {
                  throw new TimeoutException("Timed out waiting for valid responses!");
               }
            }

            if (recipients != null) {
               for (Address dest : recipients) {
                  if (!dest.equals(getAddress()) && !retval.containsKey(dest)) {
                     retval.put(dest, CacheNotFoundResponse.INSTANCE);
                  }
               }
            }

            return retval;
         });
      } else {
         throw new IllegalStateException("Should have one remote invocation future");
      }
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode,
                                                long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast)
         throws Exception {
      DeliverOrder deliverOrder = DeliverOrder.PER_SENDER;
      if (totalOrder) {
         deliverOrder = DeliverOrder.TOTAL;
      } else if (usePriorityQueue) {
         deliverOrder = DeliverOrder.NONE;
      }
      return invokeRemotely(rpcCommands, mode, timeout, responseFilter, deliverOrder, anycast);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode,
         long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast)
         throws Exception {
      if (rpcCommands == null || rpcCommands.isEmpty()) {
         // don't send if recipients list is empty
         log.trace("Destination list is empty: no need to send message");
         return InfinispanCollections.emptyMap();
      }

      if (trace)
         log.tracef("commands=%s, mode=%s, timeout=%s", rpcCommands, mode, timeout);
      boolean ignoreLeavers = mode == ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS || mode == ResponseMode.WAIT_FOR_VALID_RESPONSE;

      SingleResponseFuture[] futures = new SingleResponseFuture[rpcCommands.size()];
      int i = 0;
      for (Map.Entry<Address, ReplicableCommand> entry : rpcCommands.entrySet()) {
         org.jgroups.Address recipient = toJGroupsAddress(entry.getKey());
         ReplicableCommand command = entry.getValue();
         SingleResponseFuture future = dispatcher.invokeRemoteCommand(recipient, command, toJGroupsMode(mode),
               timeout, deliverOrder);
         futures[i] = future;
         i++;
      }

      if (mode.isAsynchronous())
         return InfinispanCollections.emptyMap();

      CompletableFuture<Void> bigFuture = CompletableFuture.allOf(futures);
      CompletableFutures.await(bigFuture);

      List<Rsp<Response>> rsps = new ArrayList<>();
      for (SingleResponseFuture future : futures) {
         rsps.add(future.get());
      }

      Map<Address, Response> retval = new HashMap<>(rsps.size());
      boolean hasResponses = false;
      for (Rsp<Response> rsp : rsps) {
         Address sender = fromJGroupsAddress(rsp.getSender());
         Response response = checkRsp(rsp, sender, responseFilter != null, ignoreLeavers);
         if (response != null) {
            retval.put(sender, response);
            hasResponses = true;
         }
      }

      if (!hasResponses) {
         // It is possible for their to be no valid response if we ignored leavers and
         // all of our targets left
         // If all the targets were suspected we didn't have a timeout
         throw new TimeoutException("Timed out waiting for valid responses!");
      }
      return retval;
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception {
      log.tracef("About to send to backups %s, command %s", backups, rpcCommand);
      Buffer buf = dispatcher.marshallCall(dispatcher.getMarshaller(), rpcCommand);
      Map<XSiteBackup, Future<Object>> syncBackupCalls = new HashMap<>(backups.size());
      for (XSiteBackup xsb : backups) {
         SiteMaster recipient = new SiteMaster(xsb.getSiteName());
         if (xsb.isSync()) {
            RequestOptions sync = new RequestOptions(org.jgroups.blocks.ResponseMode.GET_ALL, xsb.getTimeout());
            Message msg = CommandAwareRpcDispatcher.constructMessage(buf, recipient,
                  org.jgroups.blocks.ResponseMode.GET_ALL, false, DeliverOrder.NONE);
            syncBackupCalls.put(xsb, dispatcher.sendMessageWithFuture(msg, sync));
         } else {
            RequestOptions async = new RequestOptions(org.jgroups.blocks.ResponseMode.GET_NONE, xsb.getTimeout());
            Message msg = CommandAwareRpcDispatcher.constructMessage(buf, recipient,
                  org.jgroups.blocks.ResponseMode.GET_NONE, false, DeliverOrder.PER_SENDER);
            dispatcher.sendMessage(msg, async);
         }
      }
      return new JGroupsBackupResponse(syncBackupCalls, timeService);
   }

   private static org.jgroups.blocks.ResponseMode toJGroupsMode(ResponseMode mode) {
      switch (mode) {
         case ASYNCHRONOUS:
            return org.jgroups.blocks.ResponseMode.GET_NONE;
         case WAIT_FOR_VALID_RESPONSE:
            return org.jgroups.blocks.ResponseMode.GET_FIRST;
         case SYNCHRONOUS:
         case SYNCHRONOUS_IGNORE_LEAVERS:
            return org.jgroups.blocks.ResponseMode.GET_ALL;
      }
      throw new CacheException("Unknown response mode " + mode);
   }

   private RspFilter toJGroupsFilter(ResponseFilter responseFilter) {
      return responseFilter == null ? null : new JGroupsResponseFilterAdapter(responseFilter);
   }

   protected Response checkRsp(Rsp<Response> rsp, Address sender, boolean ignoreTimeout,
                               boolean ignoreLeavers) {
      Response response;
      if (rsp.wasReceived()) {
         if (rsp.hasException()) {
            log.tracef(rsp.getException(), "Unexpected exception from %s", sender);
            throw log.remoteException(sender, rsp.getException());
         } else {
            // TODO We should handle CacheNotFoundResponse exactly the same way as wasSuspected
            response = checkResponse(rsp.getValue(), sender, true);
         }
      } else if (rsp.wasSuspected()) {
         response = checkResponse(CacheNotFoundResponse.INSTANCE, sender, ignoreLeavers);
      } else {
         if (!ignoreTimeout) throw new TimeoutException("Replication timeout for " + sender);
         response = null;
      }

      return response;
   }

   // ------------------------------------------------------------------------------------------------------------------
   // Implementations of JGroups interfaces
   // ------------------------------------------------------------------------------------------------------------------


   private interface Notify {
      void emitNotification(List<Address> oldMembers, View newView);
   }

   private class NotifyViewChange implements Notify {
      @Override
      public void emitNotification(List<Address> oldMembers, View newView) {
         notifier.notifyViewChange(members, oldMembers, getAddress(), (int) newView.getViewId().getId());
      }
   }

   private class NotifyMerge implements Notify {

      @Override
      public void emitNotification(List<Address> oldMembers, View newView) {
         MergeView mv = (MergeView) newView;

         final Address address = getAddress();
         final int viewId = (int) newView.getViewId().getId();
         notifier.notifyMerge(members, oldMembers, address, viewId, getSubgroups(mv.getSubgroups()));
      }

      private List<List<Address>> getSubgroups(List<View> subviews) {
         List<List<Address>> l = new ArrayList<>(subviews.size());
         for (View v : subviews)
            l.add(fromJGroupsAddressList(v.getMembers()));
         return l;
      }
   }

   @Override
   public void viewAccepted(View newView) {
      log.debugf("New view accepted: %s", newView);
      List<org.jgroups.Address> newMembers = newView.getMembers();
      if (newMembers == null || newMembers.isEmpty()) {
         log.debugf("Received null or empty member list from JGroups channel: " + newView);
         return;
      }

      List<Address> oldMembers = members;

      // Update every view-related field while holding the lock so that waitForView only returns
      // after everything was updated.
      viewUpdateLock.lock();
      try {
         viewId = (int) newView.getViewId().getId();

         // we need a defensive copy anyway
         members = fromJGroupsAddressList(newMembers);

         // Delta view debug log for large cluster
         if (log.isDebugEnabled() && oldMembers != null) {
            List<Address> joined = new ArrayList<>(members);
            joined.removeAll(oldMembers);
            List<Address> left = new ArrayList<>(oldMembers);
            left.removeAll(members);
            log.debugf("Joined: %s, Left: %s", joined, left);
         }

         // Now that we have a view, figure out if we are the isCoordinator
         coordinator = fromJGroupsAddress(newView.getCreator());
         isCoordinator = coordinator != null && coordinator.equals(getAddress());

         // Wake up any threads that are waiting to know about who the isCoordinator is
         // do it before the notifications, so if a listener throws an exception we can still start
         viewUpdateCondition.signalAll();
      } finally {
         viewUpdateLock.unlock();
      }

      // now notify listeners - *after* updating the isCoordinator. - JBCACHE-662
      boolean hasNotifier = notifier != null;
      if (hasNotifier) {
         String clusterName = configuration.transport().clusterName();
         Notify n;
         if (newView instanceof MergeView) {
            log.receivedMergedView(clusterName, newView);
            n = new NotifyMerge();
         } else {
            log.receivedClusterView(clusterName, newView);
            n = new NotifyViewChange();
         }

         n.emitNotification(oldMembers, newView);
      }

      JGroupsAddressCache.pruneAddressCache();
   }

   @Override
   public void suspect(org.jgroups.Address suspected_mbr) {
      // no-op
   }

   @Override
   public void block() {
      // no-op since ISPN-83 has been resolved
   }

   @Override
   public void unblock() {
      // no-op since ISPN-83 has been resolved
   }

   // ------------------------------------------------------------------------------------------------------------------
   // Helpers to convert between Address types
   // ------------------------------------------------------------------------------------------------------------------

   protected static org.jgroups.Address toJGroupsAddress(Address a) {
      return ((JGroupsAddress) a).address;
   }

   static Address fromJGroupsAddress(final org.jgroups.Address addr) {
      return JGroupsAddressCache.fromJGroupsAddress(addr);
   }

   private List<org.jgroups.Address> toJGroupsAddressListExcludingSelf(Collection<Address> list, boolean totalOrder) {
      if (list == null)
         return null;
      if (list.isEmpty())
         return InfinispanCollections.emptyList();

      List<org.jgroups.Address> retval = new ArrayList<>(list.size());
      boolean ignoreSelf = !totalOrder; //in total order, we need to send the message to ourselves!
      Address self = getAddress();
      for (Address a : list) {
         if (!ignoreSelf || !a.equals(self)) {
            retval.add(toJGroupsAddress(a));
         } else {
            ignoreSelf = false; // short circuit address equality for future iterations
         }
      }

      return retval;
   }

   private static List<Address> fromJGroupsAddressList(List<org.jgroups.Address> list) {
      if (list == null || list.isEmpty())
         return InfinispanCollections.emptyList();

      List<Address> retval = new ArrayList<>(list.size());
      for (org.jgroups.Address a : list)
         retval.add(fromJGroupsAddress(a));
      return Collections.unmodifiableList(retval);
   }

   // mainly for unit testing

   public CommandAwareRpcDispatcher getCommandAwareRpcDispatcher() {
      return dispatcher;
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   public final void checkTotalOrderSupported() {
      //For replicated and distributed tx caches, we use TOA as total order protocol.
      if (channel.getProtocolStack().findProtocol(TOA.class) == null) {
         throw new CacheConfigurationException("In order to support total order based transaction, the TOA protocol " +
                                                "must be present in the JGroups's config.");
      }
   }
}
