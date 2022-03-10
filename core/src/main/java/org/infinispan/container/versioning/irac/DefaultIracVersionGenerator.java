package org.infinispan.container.versioning.irac;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commons.util.Version;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.GlobalStateManagerImpl;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteNamedCache;

/**
 * Default implementation of {@link IracVersionGenerator}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracVersionGenerator implements IracVersionGenerator {

   private static final Log log = LogFactory.getLog(DefaultIracVersionGenerator.class);
   private static final Pattern PROPERTY_PATTERN = Pattern.compile("(\\d+)_(.*)$");
   private static final AtomicIntegerFieldUpdater<DefaultIracVersionGenerator> TOPOLOGY_UPDATED = AtomicIntegerFieldUpdater
         .newUpdater(DefaultIracVersionGenerator.class, "topologyId");

   private final Map<Integer, IracEntryVersion> segmentVersion;
   private final BiFunction<Integer, IracEntryVersion, IracEntryVersion> incrementAndGet = this::incrementAndGet;
   private final Function<Integer, IracEntryVersion> createFunction = segment -> newVersion();
   @Inject RpcManager rpcManager;
   @Inject GlobalStateManager globalStateManager;
   @Inject CommandsFactory commandsFactory;
   private ByteString localSite;
   private volatile int topologyId = 1;

   public DefaultIracVersionGenerator(int numberOfSegments) {
      segmentVersion = new ConcurrentHashMap<>(numberOfSegments);
   }

   @Start
   @Override
   public void start() {
      rpcManager.getTransport().checkCrossSiteAvailable();
      localSite = XSiteNamedCache.cachedByteString(rpcManager.getTransport().localSiteName());
      globalStateManager.readScopedState(scope()).ifPresent(this::loadState);
   }

   @Stop
   @Override
   public void stop() {
      globalStateManager.writeScopedState(writeState());
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      return new IracMetadata(localSite, segmentVersion.compute(segment, incrementAndGet));
   }

   @Override
   public IracMetadata generateMetadataWithCurrentVersion(int segment) {
      return new IracMetadata(localSite, segmentVersion.computeIfAbsent(segment, createFunction));
   }

   @Override
   public IracMetadata generateNewMetadata(int segment, IracEntryVersion versionSeen) {
      if (versionSeen == null) {
         return generateNewMetadata(segment);
      }
      int vTopology = versionSeen.getTopology(localSite);
      if (vTopology > topologyId) {
         updateTopology(vTopology);
      }
      IracEntryVersion version = segmentVersion.compute(segment, (s, currentVersion) ->
            currentVersion == null ?
                  versionSeen.increment(localSite, topologyId) :
                  currentVersion.merge(versionSeen).increment(localSite, topologyId));
      return new IracMetadata(localSite, version);
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      if (remoteVersion == null) {
         return;
      }
      segmentVersion.merge(segment, remoteVersion, IracEntryVersion::merge);
      updateTopology(remoteVersion.getTopology(localSite));
   }

   @Override
   public void onTopologyChange(CacheTopology newTopology) {
      TOPOLOGY_UPDATED.incrementAndGet(this);
      if (newTopology.getPhase().isRebalance()) {
         IracUpdateVersionCommand cmd = commandsFactory.buildIracUpdateVersionCommand(peek());
         rpcManager.sendToAll(cmd, DeliverOrder.NONE);
      }
   }

   public Map<Integer, IracEntryVersion> peek() {
      // make a copy. onTopologyChange() uses this method and avoids marshalling problems
      return new HashMap<>(segmentVersion);
   }

   private void updateTopology(int newTopology) {
      int currentTopology = topologyId;
      while (newTopology > currentTopology && !TOPOLOGY_UPDATED.compareAndSet(this, currentTopology, newTopology)) {
         currentTopology = topologyId;
      }
   }

   private IracEntryVersion newVersion() {
      return IracEntryVersion.newVersion(localSite, TopologyIracVersion.newVersion(topologyId));
   }

   private IracEntryVersion incrementAndGet(int segment, IracEntryVersion currentVersion) {
      return currentVersion == null ? newVersion() : currentVersion.increment(localSite, topologyId);
   }

   private String scope() {
      return "___irac_version_" + commandsFactory.getCacheName();
   }

   private void loadState(ScopedPersistentState state) {
      assert Version.getVersion().equals(state.getProperty(GlobalStateManagerImpl.VERSION));
      state.forEach((segmentAndSite, versionString) -> {
         Matcher result = PROPERTY_PATTERN.matcher(segmentAndSite);
         if (!result.find()) {
            //other data, @version and so on
            return;
         }
         int segment = Integer.parseInt(result.group(1));
         String site = result.group(2);
         TopologyIracVersion v = TopologyIracVersion.fromString(versionString);
         if (v == null) {
            return;
         }
         IracEntryVersion partialVersion = IracEntryVersion.newVersion(XSiteNamedCache.cachedByteString(site), v);
         segmentVersion.compute(segment, (seg, version) -> version == null ? partialVersion : version.merge(partialVersion));
      });
      if (log.isTraceEnabled()) {
         log.tracef("Read state (%s entries): %s", segmentVersion.size(), segmentVersion);
      }
   }

   private ScopedPersistentState writeState() {
      if (log.isTraceEnabled()) {
         log.tracef("Write state (%s entries): %s", segmentVersion.size(), segmentVersion);
      }
      ScopedPersistentStateImpl state = new ScopedPersistentStateImpl(scope());
      state.setProperty(GlobalStateManagerImpl.VERSION, Version.getVersion());
      segmentVersion.forEach((segment, version) -> {
         String prefix = segment + "_";
         version.forEach((site, v) -> state.setProperty(prefix + site, v.toString()));
      });

      return state;
   }
}
