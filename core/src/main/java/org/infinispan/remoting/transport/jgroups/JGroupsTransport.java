package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.remoting.transport.jgroups.JGroupsAddressCache.fromJGroupsAddress;
import static org.infinispan.util.logging.Log.CLUSTER;
import static org.infinispan.util.logging.Log.CONTAINER;
import static org.infinispan.util.logging.Log.XSITE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.management.ObjectName;
import javax.sql.DataSource;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TracedCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.commons.util.logging.TraceException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.external.JGroupsProtocolComponent;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.ObjectNameKeys;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.AbstractRequest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.EmptyRaftManager;
import org.infinispan.remoting.transport.impl.FilterMapResponseCollector;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.MultiTargetRequest;
import org.infinispan.remoting.transport.impl.Request;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingleTargetRequest;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.remoting.transport.impl.SiteUnreachableXSiteResponse;
import org.infinispan.remoting.transport.impl.XSiteResponseImpl;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteRequest;
import org.jgroups.BytesMessage;
import org.jgroups.ChannelListener;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.View;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.fork.ForkChannel;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.FORK;
import org.jgroups.protocols.relay.RELAY;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.RouteStatusListener;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SocketFactory;

/**
 * An encapsulation of a JGroups transport. JGroups transports can be configured using a variety of methods, usually by
 * passing in one of the following properties:
 * <ul>
 * <li><code>configurationString</code> - a JGroups configuration String</li>
 * <li><code>configurationXml</code> - JGroups configuration XML as a String</li>
 * <li><code>configurationFile</code> - String pointing to a JGroups XML configuration file</li>
 * <li><code>channelLookup</code> - Fully qualified class name of a
 * {@link JGroupsChannelLookup} instance</li>
 * </ul>
 * These are normally passed in as Properties in
 * {@link TransportConfigurationBuilder#withProperties(Properties)} or
 * in the Infinispan XML configuration file.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@JGroupsProtocolComponent("JGroupsMetricsMetadata")
public class JGroupsTransport implements Transport {
   public static final String CONFIGURATION_STRING = "configurationString";
   public static final String CONFIGURATION_XML = "configurationXml";
   public static final String CONFIGURATION_FILE = "configurationFile";
   public static final String CHANNEL_LOOKUP = "channelLookup";
   public static final String CHANNEL_CONFIGURATOR = "channelConfigurator";
   public static final String SOCKET_FACTORY = "socketFactory";
   public static final String DATA_SOURCE = "dataSource";
   public static final short REQUEST_FLAGS_UNORDERED =
         (short) (Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value());
   public static final short REQUEST_FLAGS_UNORDERED_NO_FC = (short) (REQUEST_FLAGS_UNORDERED | Message.Flag.NO_FC.value());
   public static final short REQUEST_FLAGS_PER_SENDER = Message.Flag.NO_TOTAL_ORDER.value();
   public static final short REQUEST_FLAGS_PER_SENDER_NO_FC = (short) (REQUEST_FLAGS_PER_SENDER | Message.Flag.NO_FC.value());
   public static final short REPLY_FLAGS =
         (short) (Message.Flag.NO_FC.value() | Message.Flag.OOB.value() |
               Message.Flag.NO_TOTAL_ORDER.value());
   protected static final String DEFAULT_JGROUPS_CONFIGURATION_FILE = "default-configs/default-jgroups-udp.xml";
   public static final Log log = LogFactory.getLog(JGroupsTransport.class);
   private static final CompletableFuture<Map<Address, Response>> EMPTY_RESPONSES_FUTURE =
         CompletableFuture.completedFuture(Collections.emptyMap());
   private static final short CORRELATOR_ID = (short) 0;
   private static final short HEADER_ID = ClassConfigurator.getProtocolId(RequestCorrelator.class);
   private static final byte REQUEST = 0;
   private static final byte RESPONSE = 1;
   private static final byte SINGLE_MESSAGE = 2;
   private static final byte EMPTY_MESSAGE_BYTE = 0;
   private static final ByteBuffer EMPTY_MESSAGE_BUFFER = ByteBufferImpl.create(new byte[]{EMPTY_MESSAGE_BYTE});

   @Inject protected GlobalConfiguration configuration;
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   protected Marshaller marshaller;
   @Inject protected CacheManagerNotifier notifier;
   @Inject protected TimeService timeService;
   @Inject protected InboundInvocationHandler invocationHandler;
   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   protected ScheduledExecutorService timeoutExecutor;
   @Inject @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   protected ExecutorService nonBlockingExecutor;
   @Inject protected CacheManagerJmxRegistration jmxRegistration;
   @Inject protected JGroupsMetricsManager metricsManager;
   @Inject InfinispanTelemetry telemetry;

   private final Lock viewUpdateLock = new ReentrantLock();
   private final Condition viewUpdateCondition = viewUpdateLock.newCondition();
   private final ThreadPoolProbeHandler probeHandler;
   private final ChannelCallbacks channelCallbacks = new ChannelCallbacks();
   protected boolean connectChannel = true, disconnectChannel = true, closeChannel = true;
   protected TypedProperties props;
   protected JChannel channel;
   protected Address address;
   protected Address physicalAddress;
   // these members are not valid until we have received the first view on a second thread
   // and channelConnectedLatch is signaled
   protected volatile ClusterView clusterView =
         new ClusterView(ClusterView.INITIAL_VIEW_ID, Collections.emptyList(), null);
   private CompletableFuture<Void> nextViewFuture = new CompletableFuture<>();
   private RequestRepository requests;
   private final Map<String, SiteUnreachableReason> unreachableSites;
   private String localSite;
   private volatile RaftManager raftManager = EmptyRaftManager.INSTANCE;

   // ------------------------------------------------------------------------------------------------------------------
   // Lifecycle and setup stuff
   // ------------------------------------------------------------------------------------------------------------------
   private boolean running;

   public static FORK findFork(JChannel channel) {
      return channel.getProtocolStack().findProtocol(FORK.class);
   }

   /**
    * This form is used when the transport is created by an external source and passed in to the GlobalConfiguration.
    *
    * @param channel created and running channel to use
    */
   public JGroupsTransport(JChannel channel) {
      this();
      this.channel = channel;
      if (channel == null)
         throw new IllegalArgumentException("Cannot deal with a null channel!");
      if (channel.isConnected())
         throw new IllegalArgumentException("Channel passed in cannot already be connected!");
   }

   public JGroupsTransport() {
      this.probeHandler = new ThreadPoolProbeHandler();
      this.unreachableSites = new ConcurrentHashMap<>();
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand command,
                                                                        ResponseMode mode, long timeout,
                                                                        ResponseFilter responseFilter,
                                                                        DeliverOrder deliverOrder,
                                                                        boolean anycast) {
      if (recipients != null && recipients.isEmpty()) {
         // don't send if recipients list is empty
         log.tracef("Destination list is empty: no need to send command %s", command);
         return EMPTY_RESPONSES_FUTURE;
      }

      ClusterView view = this.clusterView;
      List<Address> localMembers = view.getMembers();
      int membersSize = localMembers.size();
      boolean ignoreLeavers =
            mode == ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS || mode == ResponseMode.WAIT_FOR_VALID_RESPONSE;
      boolean sendStaggeredRequest = mode == ResponseMode.WAIT_FOR_VALID_RESPONSE &&
            deliverOrder == DeliverOrder.NONE && recipients != null && recipients.size() > 1 && timeout > 0;
      // ISPN-6997: Never allow a switch from anycast -> broadcast and broadcast -> unicast
      // We're still keeping the option to switch for a while in case forcing a broadcast in a 2-node replicated cache
      // impacts performance significantly. If that is the case, we may need to use UNICAST3.closeConnection() instead.
      boolean broadcast = recipients == null;

      if (recipients == null && membersSize == 1) {
         // don't send if recipients list is empty
         log.tracef("The cluster has a single node: no need to broadcast command %s", command);
         return EMPTY_RESPONSES_FUTURE;
      }

      Address singleTarget = computeSingleTarget(recipients, localMembers, membersSize, broadcast);

      if (address.equals(singleTarget)) {
         log.tracef("Skipping request to self for command %s", command);
         return EMPTY_RESPONSES_FUTURE;
      }

      if (mode.isAsynchronous()) {
         // Asynchronous RPC. Send the message, but don't wait for responses.
         return performAsyncRemoteInvocation(recipients, command, deliverOrder, broadcast, singleTarget);
      }

      Collection<Address> actualTargets = broadcast ? localMembers : recipients;
      return performSyncRemoteInvocation(actualTargets, command, mode, timeout, responseFilter, deliverOrder,
            ignoreLeavers, sendStaggeredRequest, broadcast, singleTarget);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      if (destination.equals(address)) { //removed requireNonNull. this will throw a NPE in that case
         if (log.isTraceEnabled())
            log.tracef("%s not sending command to self: %s", address, command);
         return;
      }
      logCommand(command, destination);
      sendCommand(destination, command, Request.NO_REQUEST_ID, deliverOrder, true, true);
   }

   @Override
   public void sendToMany(Collection<Address> targets, ReplicableCommand command, DeliverOrder deliverOrder) {
      if (targets == null) {
         logCommand(command, "all");
         sendCommandToAll(command, Request.NO_REQUEST_ID, deliverOrder);
      } else {
         logCommand(command, targets);
         sendCommand(targets, command, Request.NO_REQUEST_ID, deliverOrder, true);
      }
   }

   @Override
   @Deprecated(forRemoval=true)
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> commands, ResponseMode mode,
                                                long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder,
                                                boolean anycast)
         throws Exception {
      if (commands == null || commands.isEmpty()) {
         // don't send if recipients list is empty
         log.trace("Destination list is empty: no need to send message");
         return Collections.emptyMap();
      }

      if (mode.isSynchronous()) {
         MapResponseCollector collector = MapResponseCollector.validOnly(commands.size());
         CompletionStage<Map<Address, Response>> request =
               invokeCommands(commands.keySet(), commands::get, collector, deliverOrder, timeout, TimeUnit.MILLISECONDS);

         try {
            return CompletableFutures.await(request.toCompletableFuture());
         } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            cause.addSuppressed(new TraceException());
            throw Util.rewrapAsCacheException(cause);
         }
      } else {
         commands.forEach(
               (a, command) -> {
                  logCommand(command, a);
                  sendCommand(a, command, Request.NO_REQUEST_ID, deliverOrder, true, true);
               });
         return Collections.emptyMap();
      }
   }

   @Override
   public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteRequest<O> rpcCommand) {
      assert !localSite.equals(backup.getSiteName()) : "sending to local site";
      if (unreachableSites.containsKey(backup.getSiteName())) {
         // fail fast if we have thread handling a SITE_UNREACHABLE event.
         return new SiteUnreachableXSiteResponse<>(backup, timeService);
      }
      Address recipient = JGroupsAddressCache.fromJGroupsAddress(new SiteMaster(backup.getSiteName()));
      long requestId = requests.newRequestId();
      logRequest(requestId, rpcCommand, recipient, "backup");
      SingleSiteRequest<ValidResponse> request =
            new SingleSiteRequest<>(SingleResponseCollector.validOnly(), requestId, requests, backup.getSiteName());
      addRequest(request);

      DeliverOrder order = backup.isSync() ? DeliverOrder.NONE : DeliverOrder.PER_SENDER;
      long timeout = backup.getTimeout();
      XSiteResponseImpl<O> xSiteResponse = new XSiteResponseImpl<>(timeService, backup);
      try {
         traceRequest(request, rpcCommand);
         sendCommand(recipient, rpcCommand, request.getRequestId(), order, false, false);
         if (timeout > 0) {
            request.setTimeout(timeoutExecutor, timeout, TimeUnit.MILLISECONDS);
         }
         request.whenComplete(xSiteResponse);
      } catch (Throwable t) {
         request.cancel(true);
         xSiteResponse.completeExceptionally(t);
      }
      return xSiteResponse;
   }

   @Override
   public boolean isCoordinator() {
      return clusterView.isCoordinator();
   }

   @Override
   public Address getCoordinator() {
      return clusterView.getCoordinator();
   }

   @Override
   public Address getAddress() {
      return address;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      if (physicalAddress == null && channel != null) {
         var addr = findPhysicalAddress(channel.getAddress());
         if (addr.isEmpty()) {
            return Collections.emptyList();
         }
         physicalAddress = new JGroupsAddress(addr.get());
      }
      return Collections.singletonList(physicalAddress);
   }

   @Override
   public List<Address> getMembers() {
      return clusterView.getMembers();
   }

   @Override
   public List<Address> getMembersPhysicalAddresses() {
      if (channel == null) {
         return Collections.emptyList();
      }
      return Arrays.stream(channel.getView().getMembersRaw())
            .map(this::findPhysicalAddress)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(JGroupsAddress::new)
            .collect(Collectors.toList());
   }

   @Override
   public boolean isMulticastCapable() {
      return channel.getProtocolStack().getTransport().supportsMulticasting();
   }

   @Override
   public void checkCrossSiteAvailable() throws CacheConfigurationException {
      if (localSite == null) {
         throw CLUSTER.crossSiteUnavailable();
      }
   }

   @Override
   public String localSiteName() {
      return localSite;
   }

   @Override
   public String localNodeName() {
      if (channel == null) {
         return Transport.super.localNodeName();
      }
      return channel.getName();
   }

   @Start
   @Override
   public void start() {
      if (running)
         throw new IllegalStateException("Two or more cache managers are using the same JGroupsTransport instance");

      probeHandler.updateThreadPool(nonBlockingExecutor);
      props = TypedProperties.toTypedProperties(configuration.transport().properties());
      requests = new RequestRepository();

      initChannel();

      channel.setUpHandler(channelCallbacks);
      setXSiteViewListener(channelCallbacks);

      startJGroupsChannelIfNeeded();

      waitForInitialNodes();
      channel.getProtocolStack().getTransport().registerProbeHandler(probeHandler);
      localSite = findRelay2().map(RELAY::site).orElse(null);
      running = true;
   }

   protected void initChannel() {
      TransportConfiguration transportCfg = configuration.transport();
      if (channel == null) {
         buildChannel();
         if (connectChannel) {
            // Cannot change the name if the channelLookup already connected the channel
            String transportNodeName = transportCfg.nodeName();
            if (transportNodeName != null && !transportNodeName.isEmpty()) {
               channel.setName(transportNodeName);
            }
         }
      }

      channel.addChannelListener(channelCallbacks);

      // Channel.LOCAL *must* be set to false so we don't see our own messages - otherwise
      // invalidations targeted at remote instances will be received by self.
      // NOTE: total order needs to deliver own messages. the invokeRemotely method has a total order boolean
      //       that when it is false, it discard our own messages, maintaining the property needed
      channel.setDiscardOwnMessages(false);

      // if we have a TopologyAwareConsistentHash, we need to set our own address generator in JGroups
      if (transportCfg.hasTopologyInfo()) {
         // We can do this only if the channel hasn't been started already
         if (connectChannel) {
            channel.addAddressGenerator(channelCallbacks);
         } else {
             verifyChannelTopology(channel.address(), transportCfg);
         }
      }
      initRaftManager();
   }

   private static void verifyChannelTopology(org.jgroups.Address jgroupsAddress, TransportConfiguration transportCfg) {
       if (jgroupsAddress instanceof ExtendedUUID) {
          JGroupsTopologyAwareAddress address = new JGroupsTopologyAwareAddress((ExtendedUUID) jgroupsAddress);
          if (!address.matches(transportCfg.siteId(), transportCfg.rackId(), transportCfg.machineId())) {
             throw new CacheException(
                   "Topology information does not match the one set by the provided JGroups channel");
          }
       } else {
          throw new CacheException("JGroups address does not contain topology coordinates");
       }
   }

   private void initRaftManager() {
      TransportConfiguration transportCfg = configuration.transport();
      if (RaftUtil.isRaftAvailable()) {
         if (transportCfg.nodeName() == null || transportCfg.nodeName().isEmpty()) {
            log.raftProtocolUnavailable("transport.node-name is not set.");
            return;
         }
         if (transportCfg.raftMembers().isEmpty()) {
            log.raftProtocolUnavailable("transport.raft-members is not set.");
            return;
         }
         // HACK!
         // TODO improve JGroups code so we have access to the key stored
         byte[] key = org.jgroups.util.Util.stringToBytes("raft-id");
         byte[] value = org.jgroups.util.Util.stringToBytes(transportCfg.nodeName());
         if (connectChannel) {
            channel.addAddressGenerator(() -> ExtendedUUID.randomUUID(channel.getName()).put(key, value));
         } else {
            org.jgroups.Address addr = channel.getAddress();
            if (addr instanceof ExtendedUUID) {
               if (!Arrays.equals(((ExtendedUUID) addr).get(key), value)) {
                  log.raftProtocolUnavailable("non-managed JGroups channel does not have 'raft-id' set.");
                  return;
               }
            }
         }
         insertForkIfMissing();
         raftManager = new JGroupsRaftManager(configuration, channel);
         raftManager.start();
         log.raftProtocolAvailable();
      }
   }

   private void insertForkIfMissing() {
      if (findFork(channel) != null) {
         return;
      }
      ProtocolStack protocolStack = channel.getProtocolStack();
      var relay2 = findRelay2();
      if (relay2.isPresent()) {
         protocolStack.insertProtocolInStack(new FORK(), relay2.get(), ProtocolStack.Position.BELOW);
      } else {
         protocolStack.addProtocol(new FORK());
      }
   }

   private void setXSiteViewListener(RouteStatusListener listener) {
      findRelay2().ifPresent(relay2 -> {
         relay2.setRouteStatusListener(listener);
         // if a node join, and is a site master, to a running cluster, it does not get any site up/down event.
         // there is a chance to get a duplicated log entry but it does not matter.
         Collection<String> view = getSitesView();
         if (view != null && !view.isEmpty()) {
            XSITE.receivedXSiteClusterView(view);
         }
      });
   }

   /**
    * When overwriting this method, it allows third-party libraries to create a new behavior like: After {@link
    * JChannel} has been created and before it is connected.
    */
   protected void startJGroupsChannelIfNeeded() {
      String clusterName = configuration.transport().clusterName();
      if (log.isDebugEnabled()) {
         log.debugf("JGroups protocol stack: %s\n", channel.getProtocolStack().printProtocolSpec(true));
      }

      String stack = configuration.transport().stack();
      if (stack != null) {
         CLUSTER.startingJGroupsChannel(clusterName, stack);
      } else if (!(channel instanceof ForkChannel)) {
         CLUSTER.startingJGroupsChannel(clusterName);
      }

      if (connectChannel) {
         try {
            channel.connect(clusterName);
         } catch (Exception e) {
            throw new CacheException("Unable to start JGroups Channel", e);
         }
      }
      // make sure the fields 'address' and 'clusterView' are set
      // for normal operation, receiveClusterView() is invoked during connect
      // but with the UPGRADE protocol active, it isn't the case.
      receiveClusterView(channel.getView(), true);
      registerMBeansIfNeeded(clusterName);
      if (!connectChannel) {
         // the channel was already started externally, we need to initialize our member list
         metricsManager.onChannelConnected(channel, true);
      }
      if (!(channel instanceof ForkChannel)) {
         CLUSTER.localAndPhysicalAddress(clusterName, getAddress(), getPhysicalAddresses());
      }
      telemetry.setNodeName(String.valueOf(channel.getAddress()));
   }

   // This needs to stay as a separate method to allow for substitution for Substrate
   private void registerMBeansIfNeeded(String clusterName) {
      try {
         // Normally this would be done by CacheManagerJmxRegistration but
         // the channel is not started when the cache manager starts but
         // when first cache starts, so it's safer to do it here.
         if (jmxRegistration.enabled()) {
            ObjectName namePrefix = new ObjectName(jmxRegistration.getDomain() + ":" + ObjectNameKeys.MANAGER + "=" + ObjectName.quote(configuration.cacheManagerName()));
            JmxConfigurator.registerChannel(channel, jmxRegistration.getMBeanServer(), namePrefix, clusterName, true);
         }
      } catch (Exception e) {
         throw new CacheException("Channel connected, but unable to register MBeans", e);
      }
   }

   private void waitForInitialNodes() {
      int initialClusterSize = configuration.transport().initialClusterSize();
      if (initialClusterSize <= 1)
         return;

      long timeout = configuration.transport().initialClusterTimeout();
      long remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeout);
      viewUpdateLock.lock();
      try {
         while (channel != null && channel.getView().getMembers().size() < initialClusterSize &&
               remainingNanos > 0) {
            log.debugf("Waiting for %d nodes, current view has %d", initialClusterSize,
                  channel.getView().getMembers().size());
            remainingNanos = viewUpdateCondition.awaitNanos(remainingNanos);
         }
      } catch (InterruptedException e) {
         CLUSTER.interruptedWaitingForCoordinator(e);
         Thread.currentThread().interrupt();
      } finally {
         viewUpdateLock.unlock();
      }

      if (remainingNanos <= 0) {
         throw CLUSTER.timeoutWaitingForInitialNodes(initialClusterSize, channel.getView().getMembers());
      }

      log.debugf("Initial cluster size of %d nodes reached", initialClusterSize);
   }

   // This is per CM, so the CL in use should be the CM CL
   private void buildChannel() {
      FileLookup fileLookup = FileLookupFactory.newInstance();

      // in order of preference - we first look for an external JGroups file, then a set of XML
      // properties, and finally the legacy JGroups String properties.
      String cfg;
      if (props != null) {
         if (props.containsKey(CHANNEL_LOOKUP)) {
            channelFromLookup(props.getProperty(CHANNEL_LOOKUP));
         }

         if (channel == null && props.containsKey(CHANNEL_CONFIGURATOR)) {
            channelFromConfigurator((JGroupsChannelConfigurator) props.get(CHANNEL_CONFIGURATOR));
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
               throw CLUSTER.jgroupsConfigurationNotFound(cfg);
            } else if (confs.size() > 1) {
               CLUSTER.ambiguousConfigurationFiles(Util.toStr(confs));
            }
            try {
               URL url = confs.iterator().next();
               channel = new JChannel(url.openStream());
            } catch (Exception e) {
               throw CLUSTER.errorCreatingChannelFromConfigFile(cfg, e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_XML)) {
            cfg = props.getProperty(CONFIGURATION_XML);
            try {
               channel = new JChannel(new ByteArrayInputStream(cfg.getBytes()));
            } catch (Exception e) {
               throw CLUSTER.errorCreatingChannelFromXML(cfg, e);
            }
         }

         if (channel == null && props.containsKey(CONFIGURATION_STRING)) {
            cfg = props.getProperty(CONFIGURATION_STRING);
            try {
               channel = new JChannel(new ByteArrayInputStream(cfg.getBytes()));
            } catch (Exception e) {
               throw CLUSTER.errorCreatingChannelFromConfigString(cfg, e);
            }
         }
         if (channel == null && configuration.transport().stack() != null) {
            channelFromConfigurator(configuration.transport().jgroups().configurator(configuration.transport().stack()));
         }
      }

      if (channel == null) {
         CLUSTER.unableToUseJGroupsPropertiesProvided(props);
         try (InputStream is = fileLookup.lookupFileLocation(DEFAULT_JGROUPS_CONFIGURATION_FILE, configuration.classLoader()).openStream()) {
            channel = new JChannel(is);
         } catch (Exception e) {
            throw CLUSTER.errorCreatingChannelFromConfigFile(DEFAULT_JGROUPS_CONFIGURATION_FILE, e);
         }
      }

      if (props != null && props.containsKey(SOCKET_FACTORY) && !props.containsKey(CHANNEL_CONFIGURATOR)) {
         Protocol protocol = channel.getProtocolStack().getTopProtocol();
         protocol.setSocketFactory((SocketFactory) props.get(SOCKET_FACTORY));
      }
   }

   @SuppressWarnings("removal")
   private void channelFromLookup(String channelLookupClassName) {
      try {
         JGroupsChannelLookup lookup = Util.getInstance(channelLookupClassName, configuration.classLoader());
         channel = lookup.getJGroupsChannel(props);
         connectChannel = lookup.shouldConnect();
         disconnectChannel = lookup.shouldDisconnect();
         closeChannel = lookup.shouldClose();
      } catch (ClassCastException e) {
         CLUSTER.wrongTypeForJGroupsChannelLookup(channelLookupClassName, e);
         throw new CacheException(e);
      } catch (Exception e) {
         CLUSTER.errorInstantiatingJGroupsChannelLookup(channelLookupClassName, e);
         throw new CacheException(e);
      }
   }

   private void channelFromConfigurator(JGroupsChannelConfigurator configurator) {
      if (props.containsKey(SOCKET_FACTORY)) {
         SocketFactory socketFactory = (SocketFactory) props.get(SOCKET_FACTORY);
         if (socketFactory instanceof NamedSocketFactory) {
            ((NamedSocketFactory) socketFactory).setName(configuration.transport().clusterName());
         }
         configurator.setSocketFactory(socketFactory);
      }
      if (props.containsKey(DATA_SOURCE)) {
         @SuppressWarnings("unchecked") Supplier<DataSource> dataSourceSupplier = (Supplier<DataSource>) props.get(DATA_SOURCE);
         configurator.setDataSource(dataSourceSupplier.get());
      }
      try {
         channel = configurator.createChannel(configuration.transport().clusterName());
      } catch (Exception e) {
         throw CLUSTER.errorCreatingChannelFromConfigurator(configurator.getName(), e);
      }
   }

   protected void receiveClusterView(View newView, boolean installIfFirst) {
      // The first view is installed before returning from JChannel.connect
      // So we need to set the local address here
      if (address == null) {
         org.jgroups.Address jgroupsAddress = channel.getAddress();
         this.address = fromJGroupsAddress(jgroupsAddress);
         if (log.isTraceEnabled()) {
            String uuid = (jgroupsAddress instanceof org.jgroups.util.UUID) ?
                  ((org.jgroups.util.UUID) jgroupsAddress).toStringLong() : "N/A";
            log.tracef("Local address %s, uuid %s", jgroupsAddress, uuid);
         }
      }
      if (installIfFirst && clusterView.getViewId() != ClusterView.INITIAL_VIEW_ID) {
         return;
      }
      List<List<Address>> subGroups;
      if (newView instanceof MergeView) {
         if (!(channel instanceof ForkChannel)) {
            CLUSTER.receivedMergedView(channel.clusterName(), newView);
         }
         subGroups = new ArrayList<>();
         List<View> jgroupsSubGroups = ((MergeView) newView).getSubgroups();
         for (View group : jgroupsSubGroups) {
            subGroups.add(fromJGroupsAddressList(group.getMembers()));
         }
      } else {
         if (!(channel instanceof ForkChannel)) {
            CLUSTER.receivedClusterView(channel.clusterName(), newView);
         }
         subGroups = Collections.emptyList();
      }
      long viewId = newView.getViewId().getId();
      List<Address> members = fromJGroupsAddressList(newView.getMembers());
      if (members.isEmpty()) {
         return;
      }

      ClusterView oldView = this.clusterView;

      // Update every view-related field while holding the lock so that waitForView only returns
      // after everything was updated.
      CompletableFuture<Void> oldFuture = null;
      viewUpdateLock.lock();
      try {
         // Delta view debug log for large cluster
         if (log.isDebugEnabled() && oldView.getMembers() != null) {
            List<Address> joined = new ArrayList<>(members);
            joined.removeAll(oldView.getMembers());
            List<Address> left = new ArrayList<>(oldView.getMembers());
            left.removeAll(members);
            log.debugf("Joined: %s, Left: %s", joined, left);
         }

         this.clusterView = new ClusterView((int) viewId, members, address);

         // Create a completable future for the new view
         oldFuture = nextViewFuture;
         nextViewFuture = new CompletableFuture<>();

         // Wake up any threads that are waiting to know about who the isCoordinator is
         // do it before the notifications, so if a listener throws an exception we can still start
         viewUpdateCondition.signalAll();
      } finally {
         viewUpdateLock.unlock();

         // Complete the future for the old view
         if (oldFuture != null) {
            CompletableFuture<Void> future = oldFuture;
            nonBlockingExecutor.execute(() -> future.complete(null));
         }
      }

      // now notify listeners - *after* updating the isCoordinator. - JBCACHE-662
      boolean hasNotifier = notifier != null;
      if (hasNotifier) {
         if (!subGroups.isEmpty()) {
            final Address address1 = getAddress();
            CompletionStages.join(notifier.notifyMerge(members, oldView.getMembers(), address1, (int) viewId, subGroups));
         } else {
            CompletionStages.join(notifier.notifyViewChange(members, oldView.getMembers(), getAddress(), (int) viewId));
         }
      }

      // Targets leaving may finish some requests and potentially potentially block for a long time.
      // We don't want to block view handling, so we unblock the commands on a separate thread.
      nonBlockingExecutor.execute(() -> {
         if (requests != null) {
            requests.forEach(request -> request.onNewView(clusterView.getMembersSet()));
         }
      });

      JGroupsAddressCache.pruneAddressCache();
   }

   private static List<Address> fromJGroupsAddressList(List<org.jgroups.Address> list) {
      return list.stream()
            .map(JGroupsAddressCache::fromJGroupsAddress)
            .toList();
   }

   @Stop
   @Override
   public void stop() {
      running = false;

      if (channel != null) {
         channel.getProtocolStack().getTransport().unregisterProbeHandler(probeHandler);
      }
      raftManager.stop();
      String clusterName = configuration.transport().clusterName();
      try {
         if (disconnectChannel && channel != null && channel.isConnected()) {
            if (!(channel instanceof ForkChannel)) {
               CLUSTER.disconnectJGroups(clusterName);
            }
            channel.disconnect();
         }

         if (closeChannel && channel != null && channel.isOpen()) {
            channel.close();
         }

         unregisterMBeansIfNeeded(clusterName);
      } catch (Exception toLog) {
         CLUSTER.problemClosingChannel(toLog, clusterName);
      }

      if (requests != null) {
         requests.forEach(request -> request.cancel(CONTAINER.cacheManagerIsStopping()));
      }

      // Don't keep a reference to the channel, but keep the address and physical address
      channel = null;
      clusterView = new ClusterView(ClusterView.FINAL_VIEW_ID, Collections.emptyList(), null);

      CompletableFuture<Void> oldFuture = null;
      viewUpdateLock.lock();
      try {
         // Create a completable future for the new view
         oldFuture = nextViewFuture;
         nextViewFuture = new CompletableFuture<>();

         // Wake up any threads blocked in waitForView()
         viewUpdateCondition.signalAll();
      } finally {
         viewUpdateLock.unlock();

         // And finally, complete the future for the old view
         if (oldFuture != null) {
            oldFuture.complete(null);
         }
      }
   }

   // This needs to stay as a separate method to allow for substitution for Substrate
   private void unregisterMBeansIfNeeded(String clusterName) throws Exception {
      if (jmxRegistration.enabled() && channel != null) {
         ObjectName namePrefix = new ObjectName(jmxRegistration.getDomain() + ":" + ObjectNameKeys.MANAGER + "=" + ObjectName.quote(configuration.cacheManagerName()));
         JmxConfigurator.unregisterChannel(channel, jmxRegistration.getMBeanServer(), namePrefix, clusterName);
      }
   }

   @Override
   public int getViewId() {
      if (channel == null)
         throw new IllegalLifecycleStateException("The cache has been stopped and invocations are not allowed!");
      return clusterView.getViewId();
   }

   @Override
   public CompletableFuture<Void> withView(int expectedViewId) {
      ClusterView view = this.clusterView;
      if (view.isViewIdAtLeast(expectedViewId))
         return CompletableFutures.completedNull();

      if (log.isTraceEnabled())
         log.tracef("Waiting for view %d, current view is %d", expectedViewId, view.getViewId());
      viewUpdateLock.lock();
      try {
         view = this.clusterView;
         if (view.isViewIdAtLeast(ClusterView.FINAL_VIEW_ID)) {
            throw new IllegalLifecycleStateException();
         } else if (view.isViewIdAtLeast(expectedViewId)) {
            return CompletableFutures.completedNull();
         } else {
            return nextViewFuture.thenCompose(nil -> withView(expectedViewId));
         }
      } finally {
         viewUpdateLock.unlock();
      }
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public Set<String> getSitesView() {
      var sites = findRelay2().map(RELAY::getCurrentSites);
      return sites.isEmpty() ? Collections.emptySet() : new TreeSet<>(sites.get());
   }

   @Override
   public boolean isSiteCoordinator() {
      return findRelay2().map(RELAY2::isSiteMaster).orElse(false);
   }

   @Override
   public Collection<Address> getRelayNodesAddress() {
      return findRelay2()
            .map(RELAY2::siteMasters)
            .map(addresses -> addresses.stream()
                  .map(JGroupsAddressCache::fromJGroupsAddress)
                  .collect(Collectors.toList()))
            .orElse(Collections.emptyList());
   }

   @Override
   public boolean isPrimaryRelayNode() {
      return findRelay2()
            .map(RELAY2::siteMasters)
            .flatMap(c -> c.stream().findFirst())
            .map(a -> Objects.equals(a, channel.getAddress()))
            .orElse(false);
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                               long timeout, TimeUnit unit) {
      if (target.equals(address)) {
         return CompletableFuture.completedFuture(collector.finish());
      }
      long requestId = requests.newRequestId();
      logRequest(requestId, command, target, "single");
      SingleTargetRequest<T> request = new SingleTargetRequest<>(collector, requestId, requests, metricsManager.trackRequest(target));
      addRequest(request);
      if (!request.onNewView(clusterView.getMembersSet())) {
         traceRequest(request, command);
         sendCommand(target, command, requestId, deliverOrder, true, false);
      }
      if (timeout > 0) {
         request.setTimeout(timeoutExecutor, timeout, unit);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                               long timeout, TimeUnit unit) {
      long requestId = requests.newRequestId();
      logRequest(requestId, command, targets, "multi");
      if (targets.isEmpty()) {
         return CompletableFuture.completedFuture(collector.finish());
      }
      Address excludedTarget = getAddress();
      MultiTargetRequest<T> request =
            new MultiTargetRequest<>(collector, requestId, requests, targets, excludedTarget, metricsManager);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      try {
         addRequest(request);
         traceRequest(request, command);
         boolean checkView = request.onNewView(clusterView.getMembersSet());
         sendCommand(targets, command, requestId, deliverOrder, checkView);
      } catch (Throwable t) {
         request.cancel(true);
         throw t;
      }
      if (timeout > 0) {
         request.setTimeout(timeoutExecutor, timeout, unit);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      long requestId = requests.newRequestId();
      logRequest(requestId, command, null, "broadcast");
      Address excludedTarget = getAddress();
      MultiTargetRequest<T> request =
            new MultiTargetRequest<>(collector, requestId, requests, clusterView.getMembers(), excludedTarget, metricsManager);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      try {
         addRequest(request);
         traceRequest(request, command);
         request.onNewView(clusterView.getMembersSet());
         sendCommandToAll(command, requestId, deliverOrder);
      } catch (Throwable t) {
         request.cancel(true);
         throw t;
      }
      if (timeout > 0) {
         request.setTimeout(timeoutExecutor, timeout, unit);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(Collection<Address> requiredTargets, ReplicableCommand command,
                                                    ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                    long timeout, TimeUnit unit) {
      long requestId = requests.newRequestId();
      logRequest(requestId, command, requiredTargets, "broadcast");
      Address excludedTarget = getAddress();
      MultiTargetRequest<T> request =
            new MultiTargetRequest<>(collector, requestId, requests, requiredTargets, excludedTarget, metricsManager);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      try {
         addRequest(request);
         traceRequest(request, command);
         request.onNewView(clusterView.getMembersSet());
         sendCommandToAll(command, requestId, deliverOrder);
      } catch (Throwable t) {
         request.cancel(true);
         throw t;
      }
      if (timeout > 0) {
         request.setTimeout(timeoutExecutor, timeout, unit);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                        long timeout, TimeUnit unit) {
      long requestId = requests.newRequestId();
      logRequest(requestId, command, targets, "staggered");
      StaggeredRequest<T> request =
            new StaggeredRequest<>(collector, requestId, requests, targets, getAddress(), command, deliverOrder,
                  timeout, unit, this);
      try {
         addRequest(request);
         traceRequest(request, command);
         request.onNewView(clusterView.getMembersSet());
         request.sendNextMessage();
      } catch (Throwable t) {
         request.cancel(true);
         throw t;
      }
      return request;
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                long timeout, TimeUnit timeUnit) {
      long requestId;
      requestId = requests.newRequestId();
      Address excludedTarget = getAddress();
      MultiTargetRequest<T> request =
            new MultiTargetRequest<>(collector, requestId, requests, targets, excludedTarget, metricsManager);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      addRequest(request);
      boolean checkView = request.onNewView(clusterView.getMembersSet());
      try {
         for (Address target : targets) {
            if (target.equals(excludedTarget))
               continue;

            ReplicableCommand command = commandGenerator.apply(target);
            logRequest(requestId, command, target, "mixed");
            traceRequest(request, command); // TODO is correct?
            sendCommand(target, command, requestId, deliverOrder, true, checkView);
         }
      } catch (Throwable t) {
         request.cancel(true);
         throw t;
      }

      if (timeout > 0) {
         request.setTimeout(timeoutExecutor, timeout, timeUnit);
      }
      return request;
   }

   @Override
   public RaftManager raftManager() {
      return raftManager;
   }

   private void addRequest(AbstractRequest<?> request) {
      try {
         requests.addRequest(request);
         if (!running) {
            request.cancel(CONTAINER.cacheManagerIsStopping());
         }
      } catch (Throwable t) {
         // Removes the request and the scheduled task, if necessary
         request.cancel(true);
         throw t;
      }
   }

   private void traceRequest(AbstractRequest<?> request, TracedCommand command) {
      var traceSpan = command.getSpanAttributes();
      if (traceSpan != null) {
         InfinispanSpan<Object> span = telemetry.startTraceRequest(command.getOperationName(), traceSpan);
         request.whenComplete(span);
      }
   }

   void sendCommand(Address target, Object command, long requestId, DeliverOrder deliverOrder,
                    boolean noRelay, boolean checkView) {
      if (checkView && !clusterView.contains(target))
         return;

      Message message = new BytesMessage(toJGroupsAddress(target));
      marshallRequest(message, command, requestId);
      setMessageFlags(message, deliverOrder, noRelay);

      send(message);
      if (noRelay) {
         // only record non cross-site messages
         metricsManager.recordMessageSent(target, message.size(), requestId == Request.NO_REQUEST_ID);
      }
   }

   private static org.jgroups.Address toJGroupsAddress(Address address) {
      return ((JGroupsAddress) address).getJGroupsAddress();
   }

   private void marshallRequest(Message message, Object command, long requestId) {
      try {
         ByteBuffer bytes = marshaller.objectToBuffer(command);
         message.setArray(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
         addRequestHeader(message, requestId);
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
   }

   private static void setMessageFlags(Message message, DeliverOrder deliverOrder, boolean noRelay) {
      short flags = encodeDeliverMode(deliverOrder);
      if (noRelay) {
         flags |= Message.Flag.NO_RELAY.value();
      }

      message.setFlag(flags, false);
      message.setFlag(Message.TransientFlag.DONT_LOOPBACK);
   }

   private void send(Message message) {
      try {
         JChannel channel = this.channel;
         if (channel != null) {
            channel.send(message);
         }
      } catch (Exception e) {
         if (running) {
            throw new CacheException(e);
         } else {
            throw CONTAINER.cacheManagerIsStopping();
         }
      }
   }

   private static void addRequestHeader(Message message, long requestId) {
      // TODO Remove the header and store the request id in the buffer
      if (requestId != Request.NO_REQUEST_ID) {
         Header header = new RequestCorrelator.Header(REQUEST, requestId, CORRELATOR_ID);
         message.putHeader(HEADER_ID, header);
      }
   }

   private static short encodeDeliverMode(DeliverOrder deliverOrder) {
      return switch (deliverOrder) {
         case PER_SENDER -> REQUEST_FLAGS_PER_SENDER;
         case PER_SENDER_NO_FC -> REQUEST_FLAGS_PER_SENDER_NO_FC;
         case NONE -> REQUEST_FLAGS_UNORDERED;
         case NONE_NO_FC -> REQUEST_FLAGS_UNORDERED_NO_FC;
      };
   }

   /**
    * @return The single target's address, or {@code null} if there are multiple targets.
    */
   private Address computeSingleTarget(Collection<Address> targets, List<Address> localMembers, int membersSize,
                                       boolean broadcast) {
      Address singleTarget;
      if (broadcast) {
         singleTarget = null;
      } else {
         if (targets == null) {
            // No broadcast means we eliminated the membersSize == 1 and membersSize > 2 possibilities
            assert membersSize == 2;
            singleTarget = localMembers.get(0).equals(address) ? localMembers.get(1) : localMembers.get(0);
         } else if (targets.size() == 1) {
            singleTarget = targets.iterator().next();
         } else {
            singleTarget = null;
         }
      }
      return singleTarget;
   }

   private CompletableFuture<Map<Address, Response>> performAsyncRemoteInvocation(Collection<Address> recipients,
                                                                                  ReplicableCommand command,
                                                                                  DeliverOrder deliverOrder,
                                                                                  boolean broadcast,
                                                                                  Address singleTarget) {
      if (broadcast) {
         logCommand(command, "all");
         sendCommandToAll(command, Request.NO_REQUEST_ID, deliverOrder);
      } else if (singleTarget != null) {
         logCommand(command, singleTarget);
         sendCommand(singleTarget, command, Request.NO_REQUEST_ID, deliverOrder, true, true);
      } else {
         logCommand(command, recipients);
         sendCommand(recipients, command, Request.NO_REQUEST_ID, deliverOrder, true);
      }
      return EMPTY_RESPONSES_FUTURE;
   }

   private CompletableFuture<Map<Address, Response>> performSyncRemoteInvocation(
         Collection<Address> targets, ReplicableCommand command, ResponseMode mode, long timeout,
         ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean ignoreLeavers, boolean sendStaggeredRequest,
         boolean broadcast, Address singleTarget) {
      CompletionStage<Map<Address, Response>> request;
      if (sendStaggeredRequest) {
         FilterMapResponseCollector collector = new FilterMapResponseCollector(responseFilter, false, targets.size());
         request = invokeCommandStaggered(targets, command, collector, deliverOrder, timeout, TimeUnit.MILLISECONDS);
      } else {
         if (singleTarget != null) {
            ResponseCollector<Map<Address, Response>> collector =
                  ignoreLeavers ? SingletonMapResponseCollector.ignoreLeavers() : SingletonMapResponseCollector.validOnly();
            request = invokeCommand(singleTarget, command, collector, deliverOrder, timeout, TimeUnit.MILLISECONDS);
         } else {
            ResponseCollector<Map<Address, Response>> collector;
            if (mode == ResponseMode.WAIT_FOR_VALID_RESPONSE) {
               collector = new FilterMapResponseCollector(responseFilter, false, targets.size());
            } else if (responseFilter != null) {
               collector = new FilterMapResponseCollector(responseFilter, true, targets.size());
            } else {
               collector = MapResponseCollector.ignoreLeavers(ignoreLeavers, targets.size());
            }
            if (broadcast) {
               request = invokeCommandOnAll(command, collector, deliverOrder, timeout, TimeUnit.MILLISECONDS);
            } else {
               request = invokeCommand(targets, command, collector, deliverOrder, timeout, TimeUnit.MILLISECONDS);
            }
         }
      }
      return request.toCompletableFuture();
   }

   public void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      logCommand(command, "all");
      sendCommandToAll(command, Request.NO_REQUEST_ID, deliverOrder);
   }

   /**
    * Send a command to the entire cluster.
    */
   private void sendCommandToAll(ReplicableCommand command, long requestId, DeliverOrder deliverOrder) {
      Message message = new BytesMessage();
      marshallRequest(message, command, requestId);
      setMessageFlags(message, deliverOrder, true);
      send(message);
      clusterView.getMembersSet().stream()
            .filter(t -> !t.equals(address))
            .forEach(t -> metricsManager.recordMessageSent(t, message.size(), requestId == Request.NO_REQUEST_ID));
   }

   private void logRequest(long requestId, Object command, Object targets, String type) {
      if (log.isTraceEnabled())
         log.tracef("%s sending %s request %d to %s: %s", address, type, requestId, targets, command);
   }

   private void logCommand(Object command, Object targets) {
      if (log.isTraceEnabled())
         log.tracef("%s sending command to %s: %s", address, targets, command);
   }

   public JChannel getChannel() {
      return channel;
   }

   private void updateSitesView(Collection<String> sitesUp, Collection<String> sitesDown) {
      var view = getSitesView();
      log.tracef("Sites view changed: up %s, down %s, new view is %s", sitesUp, sitesDown, view);
      if (!sitesUp.isEmpty()) {
         XSITE.crossSiteViewEvent("joining", String.join(", ", sitesUp));
      }
      if (!sitesDown.isEmpty()) {
         XSITE.crossSiteViewEvent("leaving", String.join(", ", sitesDown));
      }
      if (isPrimaryRelayNode()) {
         XSITE.receivedXSiteClusterView(view);
      } else {
         view = Collections.emptySet();
      }
      CompletionStages.join(notifier.notifyCrossSiteViewChanged(view, sitesUp, sitesDown));
   }

   private void siteUnreachable(String site) {
      if (unreachableSites.putIfAbsent(site, SiteUnreachableReason.SITE_UNREACHABLE_EVENT) != null) {
         // only one thread handling the events. The other threads can be "dropped".
         return;
      }
      try {
         cancelRequestsFromSite(site);
      } finally {
         unreachableSites.remove(site, SiteUnreachableReason.SITE_UNREACHABLE_EVENT);
      }

   }

   private void cancelRequestsFromSite(String site) {
      requests.forEach(request -> {
         if (request instanceof SingleSiteRequest) {
            ((SingleSiteRequest<?>) request).sitesUnreachable(site);
         }
      });
   }

   /**
    * Send a command to multiple targets.
    */
   private void sendCommand(Collection<Address> targets, ReplicableCommand command, long requestId,
                            DeliverOrder deliverOrder, boolean checkView) {
      Objects.requireNonNull(targets);
      Message message = new BytesMessage();
      marshallRequest(message, command, requestId);
      setMessageFlags(message, deliverOrder, true);

      Message copy = message;
      for (Iterator<Address> it = targets.iterator(); it.hasNext(); ) {
         Address address = it.next();

         if (checkView && !clusterView.contains(address))
            continue;

         if (address.equals(this.address))
            continue;

         copy.dest(toJGroupsAddress(address));
         send(copy);

         metricsManager.recordMessageSent(address, copy.size(), requestId == Request.NO_REQUEST_ID);

         // Send a different Message instance to each target
         if (it.hasNext()) {
            copy = copy.copy(true, true);
         }
      }
   }

   TimeService getTimeService() {
      return timeService;
   }

   ScheduledExecutorService getTimeoutExecutor() {
      return timeoutExecutor;
   }

   void processMessage(Message message) {
      org.jgroups.Address src = message.src();
      short flags = message.getFlags();
      byte[] buffer = message.getArray();
      int offset = message.getOffset();
      int length = message.getLength();
      RequestCorrelator.Header header = message.getHeader(HEADER_ID);
      byte type;
      long requestId;
      if (header != null) {
         type = header.type;
         requestId = header.requestId();
      } else {
         type = SINGLE_MESSAGE;
         requestId = Request.NO_REQUEST_ID;
      }
      if (!running) {
         if (log.isTraceEnabled())
            log.tracef("Ignoring message received before start or after stop");
         if (type == REQUEST) {
            sendResponse(src, CacheNotFoundResponse.INSTANCE, requestId, null);
         }
         return;
      }
      switch (type) {
         case SINGLE_MESSAGE:
         case REQUEST:
            processRequest(src, flags, buffer, offset, length, requestId);
            break;
         case RESPONSE:
            processResponse(src, buffer, offset, length, requestId);
            break;
         default:
            CLUSTER.invalidMessageType(type, src);
      }
   }

   private void sendResponse(org.jgroups.Address target, Response response, long requestId, Object command) {
      if (log.isTraceEnabled())
         log.tracef("%s sending response for request %d to %s: %s", getAddress(), requestId, target, response);
      ByteBuffer bytes;
      JChannel channel = this.channel;
      if (channel == null) {
         // Avoid NPEs during stop()
         return;
      }
      try {
         // If no response, then send a buffer containing a single byte. An empty payload is not possible,
         // as this can also signify to a receiver that the ForkChannel is not running on this node.
         bytes = response == null ? EMPTY_MESSAGE_BUFFER : marshaller.objectToBuffer(response);
      } catch (Throwable t) {
         try {
            // this call should succeed (all exceptions are serializable)
            Exception e = t instanceof Exception ? ((Exception) t) : new CacheException(t);
            bytes = marshaller.objectToBuffer(new ExceptionResponse(e));
         } catch (Throwable tt) {
            if (channel.isConnected()) {
               CLUSTER.errorSendingResponse(requestId, target, command);
            }
            return;
         }
      }

      try {
         Message message = new BytesMessage(target).setFlag(REPLY_FLAGS, false);
         message.setArray(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
         RequestCorrelator.Header header = new RequestCorrelator.Header(RESPONSE, requestId,
               CORRELATOR_ID);
         message.putHeader(HEADER_ID, header);

         channel.send(message);
      } catch (Throwable t) {
         if (channel.isConnected()) {
            CLUSTER.errorSendingResponse(requestId, target, command);
         }
      }
   }

   private void processRequest(org.jgroups.Address src, short flags, byte[] buffer, int offset, int length,
                               long requestId) {
      try {
         DeliverOrder deliverOrder = decodeDeliverMode(flags);
         if (src.equals(((JGroupsAddress) getAddress()).getJGroupsAddress())) {
            // DISCARD ignores the DONT_LOOPBACK flag, see https://issues.jboss.org/browse/JGRP-2205
            if (log.isTraceEnabled())
               log.tracef("Ignoring request %d from self without total order", requestId);
            return;
         }

         Object command = marshaller.objectFromByteBuffer(buffer, offset, length);
         Reply reply;
         if (requestId != Request.NO_REQUEST_ID) {
            if (log.isTraceEnabled())
               log.tracef("%s received request %d from %s: %s", getAddress(), requestId, src, command);
            reply = response -> sendResponse(src, response, requestId, command);
         } else {
            if (log.isTraceEnabled())
               log.tracef("%s received command from %s: %s", getAddress(), src, command);
            reply = Reply.NO_OP;
         }
         if (org.jgroups.util.Util.isFlagSet(flags, Message.Flag.NO_RELAY)) {
            assert command instanceof ReplicableCommand;
            invocationHandler.handleFromCluster(fromJGroupsAddress(src), (ReplicableCommand) command, reply, deliverOrder);
         } else {
            assert src instanceof SiteAddress;
            assert command instanceof XSiteRequest;
            String originSite = ((SiteAddress) src).getSite();
            invocationHandler.handleFromRemoteSite(originSite, (XSiteRequest<?>) command, reply, deliverOrder);
         }
      } catch (Throwable t) {
         CLUSTER.errorProcessingRequest(requestId, src, t);
         Exception e = t instanceof Exception ? ((Exception) t) : new CacheException(t);
         sendResponse(src, new ExceptionResponse(e), requestId, null);
      }
   }

   private void processResponse(org.jgroups.Address src, byte[] buffer, int offset, int length, long requestId) {
      try {
         Response response;
         if (length == 0) {
            // Empty buffer signals the ForkChannel with this name is not running on the remote node
            response = CacheNotFoundResponse.INSTANCE;
         } else if (length == 1 && buffer[0] == EMPTY_MESSAGE_BYTE) {
            response = SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
         } else {
            response = (Response) marshaller.objectFromByteBuffer(buffer, offset, length);
         }
         if (log.isTraceEnabled())
            log.tracef("%s received response for request %d from %s: %s", getAddress(), requestId, src, response);
         Address address = fromJGroupsAddress(src);
         requests.addResponse(requestId, address, response);
      } catch (Throwable t) {
         CLUSTER.errorProcessingResponse(requestId, src, t);
      }
   }

   private DeliverOrder decodeDeliverMode(short flags) {
      boolean oob = org.jgroups.util.Util.isFlagSet(flags, Message.Flag.OOB);
      return oob ? DeliverOrder.NONE : DeliverOrder.PER_SENDER;
   }

   private Optional<RELAY2> findRelay2() {
      return Optional.ofNullable(channel.getProtocolStack().findProtocol(RELAY2.class));
   }

   private Optional<org.jgroups.Address> findPhysicalAddress(org.jgroups.Address member) {
      return  Optional.ofNullable((org.jgroups.Address) channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, member)));
   }

   private class ChannelCallbacks implements RouteStatusListener, UpHandler, ChannelListener, AddressGenerator {

      // RouteStatusListener

      @Override
      public void sitesUp(String... sites) {
         for (String upSite : sites) {
            unreachableSites.remove(upSite, SiteUnreachableReason.SITE_DOWN_EVENT);
         }
         updateSitesView(Arrays.asList(sites), Collections.emptyList());
      }

      @Override
      public void sitesDown(String... sites) {
         List<String> requestsToCancel = new ArrayList<>(sites.length);
         for (String downSite : sites) {
            // if there is something stored in the map, do not try to cancel the requests.
            if (unreachableSites.put(downSite, SiteUnreachableReason.SITE_DOWN_EVENT) == null) {
               requestsToCancel.add(downSite);
            }
         }
         requestsToCancel.forEach(JGroupsTransport.this::cancelRequestsFromSite);
         updateSitesView(Collections.emptyList(), Arrays.asList(sites));
      }

      // UpHandler

      @Override
      public UpHandler setLocalAddress(org.jgroups.Address a) {
         //no-op
         return this;
      }

      @Override
      public Object up(Event evt) {
         switch (evt.getType()) {
            case Event.VIEW_CHANGE:
               receiveClusterView(evt.getArg(), false);
               break;
            case Event.SITE_UNREACHABLE:
               SiteMaster site_master = evt.getArg();
               String site = site_master.getSite();
               siteUnreachable(site);
               break;
         }
         return null;
      }

      @Override
      public Object up(Message msg) {
         processMessage(msg);
         return null;
      }

      @Override
      public void up(MessageBatch batch) {
         batch.forEach(message -> {
            // Removed messages are null
            if (message == null)
               return;

            // Regular (non-OOB) messages must be processed in-order
            // Normally a batch should either have only OOB or only regular messages,
            // but we check for every message to be on the safe side.
            processMessage(message);
         });
      }

      // ChannelListener

      @Override
      public void channelConnected(JChannel channel) {
         metricsManager.onChannelConnected(channel, channel == JGroupsTransport.this.channel);
      }

      @Override
      public void channelDisconnected(JChannel channel) {
         metricsManager.onChannelDisconnected(channel);
      }

      @Override
      public void channelClosed(JChannel channel) {
         // NO-OP
      }

      // AddressGenerator

      @Override
      public org.jgroups.Address generateAddress() {
         var transportCfg = configuration.transport();
         return JGroupsTopologyAwareAddress.randomUUID(channel.getName(), transportCfg.siteId(), transportCfg.rackId(),
                     transportCfg.machineId());
      }

      @Override
      public org.jgroups.Address generateAddress(String name) {
         var transportCfg = configuration.transport();
         return JGroupsTopologyAwareAddress.randomUUID(name, transportCfg.siteId(), transportCfg.rackId(),
               transportCfg.machineId());
      }
   }

   private enum SiteUnreachableReason {
      SITE_DOWN_EVENT,
      SITE_UNREACHABLE_EVENT
   }
}
