package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.remoting.transport.jgroups.JGroupsAddressCache.fromJGroupsAddress;
import static org.infinispan.util.logging.LogFactory.CLUSTER;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.TraceException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
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
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.FilterMapResponseCollector;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.MultiTargetRequest;
import org.infinispan.remoting.transport.impl.Request;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingleTargetRequest;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.View;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.RouteStatusListener;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.tom.TOA;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.MessageBatch;

/**
 * An encapsulation of a JGroups transport. JGroups transports can be configured using a variety of
 * methods, usually by passing in one of the following properties:
 * <ul>
 * <li><tt>configurationString</tt> - a JGroups configuration String</li>
 * <li><tt>configurationXml</tt> - JGroups configuration XML as a String</li>
 * <li><tt>configurationFile</tt> - String pointing to a JGroups XML configuration file</li>
 * <li><tt>channelLookup</tt> - Fully qualified class name of a
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
public class JGroupsTransport implements Transport {
   public static final String CONFIGURATION_STRING = "configurationString";
   public static final String CONFIGURATION_XML = "configurationXml";
   public static final String CONFIGURATION_FILE = "configurationFile";
   public static final String CHANNEL_LOOKUP = "channelLookup";
   public static final String CHANNEL_CONFIGURATOR = "channelConfigurator";
   public static final short REPLY_FLAGS =
         (short) (Message.Flag.NO_FC.value() | Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value());
   protected static final String DEFAULT_JGROUPS_CONFIGURATION_FILE = "default-configs/default-jgroups-udp.xml";
   public static final Log log = LogFactory.getLog(JGroupsTransport.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final CompletableFuture<Map<Address, Response>> EMPTY_RESPONSES_FUTURE =
         CompletableFuture.completedFuture(Collections.emptyMap());
   private static final short CORRELATOR_ID = (short) 0;
   private static final short HEADER_ID = ClassConfigurator.getProtocolId(RequestCorrelator.class);
   private static final byte REQUEST = 0;
   private static final byte RESPONSE = 1;
   private static final byte SINGLE_MESSAGE = 2;

   @Inject protected GlobalConfiguration configuration;
   @Inject protected StreamingMarshaller marshaller;
   @Inject protected CacheManagerNotifier notifier;
   @Inject protected TimeService timeService;
   @Inject protected InboundInvocationHandler invocationHandler;
   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   protected ScheduledExecutorService timeoutExecutor;
   @Inject @ComponentName(KnownComponentNames.REMOTE_COMMAND_EXECUTOR)
   protected ExecutorService remoteExecutor;

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
   private volatile Set<String>  sitesView = Collections.emptySet();
   private CompletableFuture<Void> nextViewFuture = new CompletableFuture<>();
   private RequestRepository requests;

   // ------------------------------------------------------------------------------------------------------------------
   // Lifecycle and setup stuff
   // ------------------------------------------------------------------------------------------------------------------
   private boolean globalStatsEnabled;
   private MBeanServer mbeanServer;
   private String domain;
   private boolean running;

   /**
    * This form is used when the transport is created by an external source and passed in to the
    * GlobalConfiguration.
    *
    * @param channel created and running channel to use
    */
   public JGroupsTransport(JChannel channel) {
      this.channel = channel;
      if (channel == null)
         throw new IllegalArgumentException("Cannot deal with a null channel!");
      if (channel.isConnected())
         throw new IllegalArgumentException("Channel passed in cannot already be connected!");
      probeHandler = new ThreadPoolProbeHandler();
   }

   public JGroupsTransport() {
      probeHandler = new ThreadPoolProbeHandler();
   }

   private static List<org.jgroups.Address> toJGroupsAddressList(Collection<Address> addresses) {
      if (addresses == null)
         return null;
      return addresses.stream().map(JGroupsTransport::toJGroupsAddress).collect(Collectors.toList());
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
      boolean totalOrder = deliverOrder == DeliverOrder.TOTAL;
      boolean sendStaggeredRequest = mode == ResponseMode.WAIT_FOR_VALID_RESPONSE &&
            deliverOrder == DeliverOrder.NONE && recipients != null && recipients.size() > 1 && timeout > 0;
      boolean rsvp = isRsvpCommand(command);
      // ISPN-6997: Never allow a switch from anycast -> broadcast and broadcast -> unicast
      // We're still keeping the option to switch for a while in case forcing a broadcast in a 2-node replicated cache
      // impacts performance significantly. If that is the case, we may need to use UNICAST3.closeConnection() instead.
      boolean broadcast = recipients == null;

      if (!totalOrder && recipients == null && membersSize == 1) {
         // don't send if recipients list is empty
         log.tracef("The cluster has a single node: no need to broadcast command %s", command);
         return EMPTY_RESPONSES_FUTURE;
      }

      Address singleTarget = computeSingleTarget(recipients, localMembers, membersSize, broadcast, totalOrder);

      if (!totalOrder && address.equals(singleTarget)) {
         log.tracef("Skipping request to self for command %s", command);
         return EMPTY_RESPONSES_FUTURE;
      }

      if (mode.isAsynchronous()) {
         // Asynchronous RPC. Send the message, but don't wait for responses.
         return performAsyncRemoteInvocation(recipients, command, deliverOrder, rsvp, broadcast, singleTarget);
      }

      Collection<Address> actualTargets = broadcast ? localMembers : recipients;
      return performSyncRemoteInvocation(actualTargets, command, mode, timeout, responseFilter, deliverOrder,
                                         ignoreLeavers, sendStaggeredRequest, broadcast, singleTarget);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder)   {
      if (destination.equals(address)) { //removed requireNonNull. this will throw a NPE in that case
         if (trace)
            log.tracef("%s not sending command to self: %s", address, command);
         return;
      }
      logCommand(command, destination);
      sendCommand(destination, command, Request.NO_REQUEST_ID, deliverOrder, isRsvpCommand(command), true, true);
   }

   @Override
   public void sendToMany(Collection<Address> targets, ReplicableCommand command, DeliverOrder deliverOrder) {
      if (targets == null) {
         logCommand(command, "all");
         sendCommandToAll(command, Request.NO_REQUEST_ID, deliverOrder, false);
      } else {
         logCommand(command, targets);
         sendCommand(targets, command, Request.NO_REQUEST_ID, deliverOrder, false, true);
      }
   }

   @Override
   @Deprecated
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
                  sendCommand(a, command, Request.NO_REQUEST_ID, deliverOrder, isRsvpCommand(command), true, true);
               });
         return Collections.emptyMap();
      }
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand command) {
      if (trace)
         log.tracef("About to send to backups %s, command %s", backups, command);
      boolean rsvp = isRsvpCommand(command);
      Map<XSiteBackup, Future<ValidResponse>> syncBackupCalls = new HashMap<>(backups.size());
      for (XSiteBackup xsb : backups) {
         Address recipient = JGroupsAddressCache.fromJGroupsAddress(new SiteMaster(xsb.getSiteName()));
         if (xsb.isSync()) {
            long timeout = xsb.getTimeout();
            long requestId = requests.newRequestId();
            logRequest(requestId, command, recipient);
            SingleSiteRequest<ValidResponse> request =
                  new SingleSiteRequest<>(SingleResponseCollector.validOnly(), requestId, requests, xsb.getSiteName());
            addRequest(request);

            try {
               sendCommand(recipient, command, request.getRequestId(), DeliverOrder.NONE, rsvp, false, false);
               if (timeout > 0) {
                  request.setTimeout(timeoutExecutor, timeout, TimeUnit.MILLISECONDS);
               }
            } catch (Throwable t) {
               request.cancel(true);
               throw t;
            }
            syncBackupCalls.put(xsb, request);
         } else {
            sendCommand(recipient, command, Request.NO_REQUEST_ID, DeliverOrder.PER_SENDER, false, false, false);
         }
      }
      return new JGroupsBackupResponse(syncBackupCalls, timeService);
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
         org.jgroups.Address addr =
               (org.jgroups.Address) channel.down(new Event(Event.GET_PHYSICAL_ADDRESS, channel.getAddress()));
         if (addr == null) {
            return Collections.emptyList();
         }
         physicalAddress = new JGroupsAddress(addr);
      }
      return Collections.singletonList(physicalAddress);
   }

   @Override
   public List<Address> getMembers() {
      return clusterView.getMembers();
   }

   @Override
   public boolean isMulticastCapable() {
      return channel.getProtocolStack().getTransport().supportsMulticasting();
   }

   @Override
   public void start() {
      probeHandler.updateThreadPool(remoteExecutor);
      props = TypedProperties.toTypedProperties(configuration.transport().properties());
      requests = new RequestRepository();

      if (log.isInfoEnabled())
         log.startingJGroupsChannel(configuration.transport().clusterName());

      initChannel();

      channel.setUpHandler(channelCallbacks);
      setXSiteViewListener(channelCallbacks);
      setSiteMasterPicker(new SiteMasterPickerImpl());

      startJGroupsChannelIfNeeded();

      waitForInitialNodes();
      channel.getProtocolStack().getTransport().registerProbeHandler(probeHandler);
      running = true;
   }

   protected void initChannel() {
      final TransportConfiguration transportCfg = configuration.transport();
      if (channel == null) {
         buildChannel();
         if (connectChannel) {
            // Cannot change the name if the channelLookup already connected the channel
            String transportNodeName = transportCfg.nodeName();
            if (transportNodeName != null && transportNodeName.length() > 0) {
               long range = Short.MAX_VALUE * 2;
               long randomInRange = (long) ((Math.random() * range) % range) + 1;
               transportNodeName = transportNodeName + "-" + randomInRange;
               channel.setName(transportNodeName);
            }
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
            channel.addAddressGenerator(() -> JGroupsTopologyAwareAddress
                  .randomUUID(channel.getName(), transportCfg.siteId(), transportCfg.rackId(),
                              transportCfg.machineId()));
         } else {
            org.jgroups.Address jgroupsAddress = channel.getAddress();
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
      }
   }

   private void setXSiteViewListener(RouteStatusListener listener) {
      RELAY2 relay2 = channel.getProtocolStack().findProtocol(RELAY2.class);
      if (relay2 != null) {
         relay2.setRouteStatusListener(listener);
      }
   }

   private void setSiteMasterPicker(SiteMasterPickerImpl siteMasterPicker) {
      RELAY2 relay2 = channel.getProtocolStack().findProtocol(RELAY2.class);
      if (relay2 != null) {
         relay2.siteMasterPicker(siteMasterPicker);
      }
   }

   private void startJGroupsChannelIfNeeded() {
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
            GlobalJmxStatisticsConfiguration jmxConfig = configuration.globalJmxStatistics();
            globalStatsEnabled = jmxConfig.enabled();
            if (globalStatsEnabled) {
               String groupName = String.format("type=channel,cluster=%s", ObjectName.quote(clusterName));
               mbeanServer = JmxUtil.lookupMBeanServer(jmxConfig.mbeanServerLookup(), jmxConfig.properties());
               domain = JmxUtil.buildJmxDomain(jmxConfig.domain(), mbeanServer, groupName);
               JmxConfigurator.registerChannel(channel, mbeanServer, domain, clusterName, true);
            }
         } catch (Exception e) {
            throw new CacheException("Channel connected, but unable to register MBeans", e);
         }
      }
      if (!connectChannel) {
         // the channel was already started externally, we need to initialize our member list
         receiveClusterView(channel.getView());
      }
      if (log.isInfoEnabled())
         log.localAndPhysicalAddress(clusterName, getAddress(), getPhysicalAddresses());
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
         log.interruptedWaitingForCoordinator(e);
         Thread.currentThread().interrupt();
      } finally {
         viewUpdateLock.unlock();
      }

      if (remainingNanos <= 0) {
         throw log.timeoutWaitingForInitialNodes(initialClusterSize, channel.getView().getMembers());
      }

      log.debugf("Initial cluster size of %d nodes reached", initialClusterSize);
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

         if (channel == null && props.containsKey(CHANNEL_CONFIGURATOR)) {
            JGroupsChannelConfigurator configurator = (JGroupsChannelConfigurator) props.get(CHANNEL_CONFIGURATOR);
            try {
               channel = configurator.createChannel();
            } catch (Exception e) {
               throw log.errorCreatingChannelFromConfigurator(configurator.getProtocolStackString(), e);
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
               channel = new JChannel(new ByteArrayInputStream(cfg.getBytes()));
            } catch (Exception e) {
               throw log.errorCreatingChannelFromConfigString(cfg, e);
            }
         }
      }

      if (channel == null) {
         log.unableToUseJGroupsPropertiesProvided(props);
         try {
            channel = new JChannel(
                  fileLookup.lookupFileLocation(DEFAULT_JGROUPS_CONFIGURATION_FILE, configuration.classLoader()));
         } catch (Exception e) {
            throw log.errorCreatingChannelFromConfigFile(DEFAULT_JGROUPS_CONFIGURATION_FILE, e);
         }
      }
   }

   protected void receiveClusterView(View newView) {
      // The first view is installed before returning from JChannel.connect
      // So we need to set the local address here
      if (address == null) {
         org.jgroups.Address jgroupsAddress = channel.getAddress();
         this.address = fromJGroupsAddress(jgroupsAddress);
         if (trace) {
            String uuid = (jgroupsAddress instanceof org.jgroups.util.UUID) ?
                          ((org.jgroups.util.UUID) jgroupsAddress).toStringLong() : "N/A";
            log.tracef("Local address %s, uuid %s", jgroupsAddress, uuid);
         }
      }
      List<List<Address>> subGroups;
      if (newView instanceof MergeView) {
         CLUSTER.receivedMergedView(channel.clusterName(), newView);
         subGroups = new ArrayList<>();
         List<View> jgroupsSubGroups = ((MergeView) newView).getSubgroups();
         for (View group : jgroupsSubGroups) {
            subGroups.add(fromJGroupsAddressList(group.getMembers()));
         }
      } else {
         CLUSTER.receivedClusterView(channel.clusterName(), newView);
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
            remoteExecutor.execute(() -> future.complete(null));
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
      remoteExecutor.execute(() -> {
         if (requests != null) {
            requests.forEach(request -> request.onNewView(clusterView.getMembersSet()));
         }
      });

      JGroupsAddressCache.pruneAddressCache();
   }

   private static List<Address> fromJGroupsAddressList(List<org.jgroups.Address> list) {
      return Collections.unmodifiableList(list.stream()
                                              .map(JGroupsAddressCache::fromJGroupsAddress)
                                              .collect(Collectors.toList()));
   }

   @Stop(priority = 120)
   @Override
   public void stop() {
      running = false;

      if (channel != null) {
         channel.getProtocolStack().getTransport().unregisterProbeHandler(probeHandler);
      }
      String clusterName = configuration.transport().clusterName();
      try {
         if (disconnectChannel && channel != null && channel.isConnected()) {
            log.disconnectJGroups(clusterName);

            // Unregistering before disconnecting/closing because
            // after that the cluster name is null
            if (globalStatsEnabled) {
               JmxConfigurator.unregisterChannel(channel, mbeanServer, domain, channel.getClusterName());
            }

            channel.disconnect();
         }
         if (closeChannel && channel != null && channel.isOpen()) {
            channel.close();
         }
      } catch (Exception toLog) {
         log.problemClosingChannel(toLog, clusterName);
      }

      if (requests != null) {
         requests.forEach(request -> request.cancel(log.cacheManagerIsStopping()));
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

   @Override
   public int getViewId() {
      if (channel == null)
         throw new CacheException("The cache has been stopped and invocations are not allowed!");
      return clusterView.getViewId();
   }

   @Override
   public CompletableFuture<Void> withView(int expectedViewId) {
      ClusterView view = this.clusterView;
      if (view.isViewIdAtLeast(expectedViewId))
         return CompletableFutures.completedNull();

      if (trace)
         log.tracef("Waiting for transaction data for view %d, current view is %d", expectedViewId,
                    view.getViewId());
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
   public void waitForView(int viewId) throws InterruptedException {
      if (channel == null)
         return;

      log.tracef("Waiting on view %d being accepted", viewId);
      long remainingNanos = Long.MAX_VALUE;
      viewUpdateLock.lock();
      try {
         while (channel != null && getViewId() < viewId && remainingNanos > 0) {
            remainingNanos = viewUpdateCondition.awaitNanos(remainingNanos);
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
   public final void checkTotalOrderSupported() {
      //For replicated and distributed tx caches, we use TOA as total order protocol.
      if (channel.getProtocolStack().findProtocol(TOA.class) == null) {
         throw new CacheConfigurationException("In order to support total order based transaction, the TOA protocol " +
                                                     "must be present in the JGroups's config.");
      }
   }

   @Override
   public Set<String> getSitesView() {
      return sitesView;
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                               long timeout, TimeUnit unit) {
      if (target.equals(address) && deliverOrder != DeliverOrder.TOTAL) {
         return CompletableFuture.completedFuture(collector.finish());
      }
      long requestId = requests.newRequestId();
      logRequest(requestId, command, target);
      SingleTargetRequest<T> request = new SingleTargetRequest<>(collector, requestId, requests, target);
      addRequest(request);
      boolean invalidTarget = request.onNewView(clusterView.getMembersSet());
      if (!invalidTarget) {
         sendCommand(target, command, requestId, deliverOrder, isRsvpCommand(command), true, false);
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
      logRequest(requestId, command, targets);
      if (targets.isEmpty()) {
         return CompletableFuture.completedFuture(collector.finish());
      }
      Address excludedTarget = deliverOrder == DeliverOrder.TOTAL ? null : getAddress();
      MultiTargetRequest<T> request =
            new MultiTargetRequest<>(collector, requestId, requests, targets, excludedTarget);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      try {
         addRequest(request);
         boolean checkView = request.onNewView(clusterView.getMembersSet());
         sendCommand(targets, command, requestId, deliverOrder, isRsvpCommand(command), checkView);
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
      logRequest(requestId, command, "all");
      Address excludedTarget = deliverOrder == DeliverOrder.TOTAL ? null : getAddress();
      MultiTargetRequest<T> request =
            new MultiTargetRequest<>(collector, requestId, requests, clusterView.getMembers(), excludedTarget);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      try {
         addRequest(request);
         request.onNewView(clusterView.getMembersSet());
         sendCommandToAll(command, requestId, deliverOrder, isRsvpCommand(command));
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
      logRequest(requestId, command, "all-required");
      Address excludedTarget = deliverOrder == DeliverOrder.TOTAL ? null : getAddress();
      MultiTargetRequest<T> request =
         new MultiTargetRequest<>(collector, requestId, requests, requiredTargets, excludedTarget);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      try {
         addRequest(request);
         request.onNewView(clusterView.getMembersSet());
         sendCommandToAll(command, requestId, deliverOrder, isRsvpCommand(command));
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
      logRequest(requestId, command, "staggered " + targets);
      StaggeredRequest<T> request =
            new StaggeredRequest<>(collector, requestId, requests, targets, getAddress(), command, deliverOrder,
                                   timeout, unit, this);
      try {
         addRequest(request);
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
            new MultiTargetRequest<>(collector, requestId, requests, targets, excludedTarget);
      // Request may be completed due to exclusion of target nodes, so only send it if it isn't complete
      if (request.isDone()) {
         return request;
      }
      addRequest(request);
      boolean checkView = request.onNewView(clusterView.getMembersSet());
      try {
         for (Address target : targets) {
            ReplicableCommand command = commandGenerator.apply(target);
            boolean rsvp = isRsvpCommand(command);
            logRequest(requestId, command, target);
            sendCommand(target, command, requestId, deliverOrder, rsvp, true, checkView);
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

   private void addRequest(AbstractRequest<?> request) {
      try {
         requests.addRequest(request);
         if (!running) {
            request.cancel(log.cacheManagerIsStopping());
         }
      } catch (Throwable t) {
         // Removes the request and the scheduled task, if necessary
         request.cancel(true);
         throw t;
      }
   }

   void sendCommand(Address target, ReplicableCommand command, long requestId, DeliverOrder deliverOrder,
                    boolean rsvp, boolean noRelay, boolean checkView) {
      if (checkView && !clusterView.contains(target))
         return;

      Message message = new Message(toJGroupsAddress(target));
      marshallRequest(message, command, requestId);
      setMessageFlags(message, deliverOrder, rsvp, noRelay);

      send(message);
   }

   private static boolean isRsvpCommand(ReplicableCommand command) {
      return command instanceof FlagAffectedCommand &&
            ((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.GUARANTEED_DELIVERY);
   }

   private static org.jgroups.Address toJGroupsAddress(Address address) {
      return ((JGroupsAddress) address).getJGroupsAddress();
   }

   private void marshallRequest(Message message, ReplicableCommand command, long requestId) {
      try {
         ByteBuffer bytes = marshaller.objectToBuffer(command);
         message.setBuffer(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
         addRequestHeader(message, requestId);
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
   }

   private static void setMessageFlags(Message message, DeliverOrder deliverOrder, boolean rsvp, boolean noRelay) {
      if (noRelay) {
         message.setFlag(Message.Flag.NO_RELAY.value());
      }
      short flags = encodeDeliverMode(deliverOrder);
      message.setFlag(flags);
      // Only the commands in total order must be received by the originator.
      if (deliverOrder != DeliverOrder.TOTAL) {
         message.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK.value());
      }
      if (rsvp) {
         message.setFlag(Message.Flag.RSVP.value());
      }
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
            throw log.cacheManagerIsStopping();
         }
      }
   }

   private void addRequestHeader(Message message, long requestId) {
      // TODO Remove the header and store the request id in the buffer
      if (requestId != Request.NO_REQUEST_ID) {
         Header header = new RequestCorrelator.Header(REQUEST, requestId, CORRELATOR_ID);
         message.putHeader(HEADER_ID, header);
      }
   }

   private static short encodeDeliverMode(DeliverOrder deliverOrder) {
      switch (deliverOrder) {
         case TOTAL:
            return Message.Flag.OOB.value();
         case PER_SENDER:
            return Message.Flag.NO_TOTAL_ORDER.value();
         case NONE:
            return (short) (Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value());
         default:
            throw new IllegalArgumentException("Unsupported deliver mode " + deliverOrder);
      }
   }

   /**
    * @return The single target's address, or {@code null} if there are multiple targets.
    */
   private Address computeSingleTarget(Collection<Address> targets, List<Address> localMembers, int membersSize,
                                       boolean broadcast, boolean totalOrder) {
      Address singleTarget;
      if (broadcast || totalOrder) {
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
                                                                                  boolean rsvp, boolean broadcast,
                                                                                  Address singleTarget) {
      if (broadcast) {
         logCommand(command, "all");
         sendCommandToAll(command, Request.NO_REQUEST_ID, deliverOrder, rsvp);
      } else if (singleTarget != null) {
         logCommand(command, singleTarget);
         sendCommand(singleTarget, command, Request.NO_REQUEST_ID, deliverOrder, rsvp, true, true);
      } else {
         logCommand(command, recipients);
         sendCommand(recipients, command, Request.NO_REQUEST_ID, deliverOrder, rsvp, true);
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
      sendCommandToAll(command, Request.NO_REQUEST_ID, deliverOrder, false);
   }

   /**
    * Send a command to the entire cluster.
    *
    * Doesn't send the command to itself unless {@code deliverOrder == TOTAL}.
    */
   private void sendCommandToAll(ReplicableCommand command, long requestId, DeliverOrder deliverOrder, boolean rsvp) {
      Message message = new Message();
      marshallRequest(message, command, requestId);
      setMessageFlags(message, deliverOrder, rsvp, true);

      if (deliverOrder == DeliverOrder.TOTAL) {
         message.dest(new AnycastAddress());
      }

      send(message);
   }

   private void logRequest(long requestId, ReplicableCommand command, Object targets) {
      if (trace)
         log.tracef("%s sending request %d to %s: %s", address, requestId, targets, command);
   }

   private void logCommand(ReplicableCommand command, Object targets) {
      if (trace)
         log.tracef("%s sending command to %s: %s", address, targets, command);
   }

   public JChannel getChannel() {
      return channel;
   }

   private void updateSitesView(Collection<String> sitesUp, Collection<String> sitesDown) {
      viewUpdateLock.lock();
      try {
         Set<String> reachableSites = new HashSet<>(sitesView);
         reachableSites.addAll(sitesUp);
         reachableSites.removeAll(sitesDown);
         log.tracef("Sites view changed: up %s, down %s, new view is %s", sitesUp, sitesDown, reachableSites);
         log.receivedXSiteClusterView(reachableSites);
         sitesView = Collections.unmodifiableSet(reachableSites);
      } finally {
         viewUpdateLock.unlock();
      }
   }

   private void siteUnreachable(String site) {
      requests.forEach(request -> {
         if (request instanceof SingleSiteRequest) {
            ((SingleSiteRequest) request).sitesUnreachable(site);
         }
      });
   }

   /**
    * Send a command to multiple targets.
    *
    * Doesn't send the command to itself unless {@code deliverOrder == TOTAL}.
    */
   private void sendCommand(Collection<Address> targets, ReplicableCommand command, long requestId,
                            DeliverOrder deliverOrder, boolean rsvp, boolean checkView) {
      Objects.requireNonNull(targets);
      Message message = new Message();
      marshallRequest(message, command, requestId);
      setMessageFlags(message, deliverOrder, rsvp, true);

      if (deliverOrder == DeliverOrder.TOTAL) {
         message.dest(new AnycastAddress(toJGroupsAddressList(targets)));
         send(message);
      } else {
         Message copy = message;
         for (Iterator<Address> it = targets.iterator(); it.hasNext(); ) {
            Address address = it.next();

            if (checkView && !clusterView.contains(address))
               continue;

            if (address.equals(getAddress()))
               continue;

            copy.dest(toJGroupsAddress(address));
            send(copy);

            // Send a different Message instance to each target
            if (it.hasNext()) {
               copy = copy.copy(true);
            }
         }
      }
   }

   TimeService getTimeService() {
      return timeService;
   }

   ScheduledExecutorService getTimeoutExecutor() {
      return timeoutExecutor;
   }

   private void processMessage(Message message) {
      org.jgroups.Address src = message.src();
      short flags = message.getFlags();
      byte[] buffer = message.rawBuffer();
      int offset = message.offset();
      int length = message.length();
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
         if (trace)
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
            log.invalidMessageType(type, src);
      }
   }

   private void sendResponse(org.jgroups.Address target, Response response, long requestId, ReplicableCommand command) {
      if (trace)
         log.tracef("%s sending response for request %d to %s: %s", getAddress(), requestId, target, response);
      ByteBuffer bytes;
      JChannel channel = this.channel;
      if (channel == null) {
         // Avoid NPEs during stop()
         return;
      }
      try {
         bytes = marshaller.objectToBuffer(response);
      } catch (Throwable t) {
         try {
            // this call should succeed (all exceptions are serializable)
            Exception e = t instanceof Exception ? ((Exception) t) : new CacheException(t);
            bytes = marshaller.objectToBuffer(new ExceptionResponse(e));
         } catch (Throwable tt) {
            if (channel.isConnected()) {
               log.errorSendingResponse(requestId, target, command);
            }
            return;
         }
      }

      try {
         Message message = new Message(target).setFlag(REPLY_FLAGS);
         message.setBuffer(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
         RequestCorrelator.Header header = new RequestCorrelator.Header(RESPONSE, requestId,
                                                                        CORRELATOR_ID);
         message.putHeader(HEADER_ID, header);

         channel.send(message);
      } catch (Throwable t) {
         if (channel.isConnected()) {
            log.errorSendingResponse(requestId, target, command);
         }
      }
   }

   private void processRequest(org.jgroups.Address src, short flags, byte[] buffer, int offset, int length,
                               long requestId) {
      try {
         DeliverOrder deliverOrder = decodeDeliverMode(flags);
         if (deliverOrder != DeliverOrder.TOTAL && src.equals(((JGroupsAddress) getAddress()).getJGroupsAddress())) {
            // DISCARD ignores the DONT_LOOPBACK flag, see https://issues.jboss.org/browse/JGRP-2205
            if (trace)
               log.tracef("Ignoring request %d from self without total order", requestId);
            return;
         }

         ReplicableCommand command = (ReplicableCommand) marshaller.objectFromByteBuffer(buffer, offset, length);
         Reply reply;
         if (requestId != Request.NO_REQUEST_ID) {
            if (trace)
               log.tracef("%s received request %d from %s: %s", getAddress(), requestId, src, command);
            reply = response -> sendResponse(src, response, requestId, command);
         } else {
            if (trace)
               log.tracef("%s received command from %s: %s", getAddress(), src, command);
            reply = Reply.NO_OP;
         }
         if (src instanceof SiteAddress) {
            String originSite = ((SiteAddress) src).getSite();
            ((XSiteReplicateCommand) command).setOriginSite(originSite);
            invocationHandler.handleFromRemoteSite(originSite, (XSiteReplicateCommand) command, reply, deliverOrder);
         } else {
            invocationHandler.handleFromCluster(fromJGroupsAddress(src), command, reply, deliverOrder);
         }
      } catch (Throwable t) {
         log.errorProcessingRequest(requestId, src);
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
         } else {
            response = (Response) marshaller.objectFromByteBuffer(buffer, offset, length);
            if (response == null) {
               response = SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
            }
         }
         if (trace)
            log.tracef("%s received response for request %d from %s: %s", getAddress(), requestId, src, response);
         Address address = fromJGroupsAddress(src);
         requests.addResponse(requestId, address, response);
      } catch (Throwable t) {
         log.errorProcessingResponse(requestId, src);
      }
   }

   private DeliverOrder decodeDeliverMode(short flags) {
      boolean noTotalOrder = Message.isFlagSet(flags, Message.Flag.NO_TOTAL_ORDER);
      boolean oob = Message.isFlagSet(flags, Message.Flag.OOB);
      if (!noTotalOrder && oob) {
         return DeliverOrder.TOTAL;
      } else if (noTotalOrder && oob) {
         return DeliverOrder.NONE;
      } else if (noTotalOrder) {
         //oob is not set at this point, but the no total order flag should.
         return DeliverOrder.PER_SENDER;
      }
      throw new IllegalArgumentException("Unable to decode order from flags " + flags);
   }

   private class ChannelCallbacks implements RouteStatusListener, UpHandler {
      @Override
      public void sitesUp(String... sites) {
         updateSitesView(Arrays.asList(sites), Collections.emptyList());
      }

      @Override
      public void sitesDown(String... sites) {
         updateSitesView(Collections.emptyList(), Arrays.asList(sites));
      }

      @Override
      public Object up(Event evt) {
         switch (evt.getType()) {
            case Event.VIEW_CHANGE:
               receiveClusterView(evt.getArg());
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
         batch.forEach((message, messages) -> processMessage(message));
      }
   }
}
