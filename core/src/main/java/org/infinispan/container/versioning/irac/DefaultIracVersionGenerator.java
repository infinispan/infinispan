package org.infinispan.container.versioning.irac;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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

/**
 * Default implementation of {@link IracVersionGenerator}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultIracVersionGenerator implements IracVersionGenerator {

   private static final Pattern PROPERTY_PATTERN = Pattern.compile("(\\d+)_(.*)$");
   private static final AtomicIntegerFieldUpdater<DefaultIracVersionGenerator> TOPOLOGY_UPDATED = AtomicIntegerFieldUpdater
         .newUpdater(DefaultIracVersionGenerator.class, "topologyId");

   private final Map<Integer, Map<String, TopologyIracVersion>> segmentVersion;
   private final String cacheName;
   @Inject RpcManager rpcManager;
   @Inject GlobalStateManager globalStateManager;
   @Inject CommandsFactory commandsFactory;
   private String localSite;
   private volatile int topologyId = 1;

   public DefaultIracVersionGenerator(String cacheName) {
      this.cacheName = cacheName;
      this.segmentVersion = new ConcurrentHashMap<>();
   }

   @Start
   @Override
   public void start() {
      rpcManager.getTransport().checkCrossSiteAvailable();
      localSite = rpcManager.getTransport().localSiteName();
      globalStateManager.readScopedState(scope()).ifPresent(this::loadState);
   }

   @Stop
   @Override
   public void stop() {
      globalStateManager.writeScopedState(writeState());
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      return new IracMetadata(localSite, new IracEntryVersion(increment(segment)));
   }

   @Override
   public IracMetadata generateMetadataWithCurrentVersion(int segment) {
      Map<String, TopologyIracVersion> v = segmentVersion.compute(segment, this::getVectorFunction);
      return new IracMetadata(localSite, new IracEntryVersion(v));
   }

   @Override
   public IracMetadata generateNewMetadata(int segment, IracEntryVersion versionSeen) {
      updateVersion(segment, versionSeen);
      return generateNewMetadata(segment);
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      if (remoteVersion == null) {
         return;
      }
      segmentVersion.merge(segment, remoteVersion.toMap(), DefaultIracVersionGenerator::mergeVectorsFunction);
      int currentTopology = topologyId;
      final int newTopology = remoteVersion.getTopology(localSite);
      while (newTopology > currentTopology && !TOPOLOGY_UPDATED.compareAndSet(this, currentTopology, newTopology)) {
         currentTopology = topologyId;
      }
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
      Map<Integer, IracEntryVersion> copy = new HashMap<>();
      segmentVersion.forEach((seg, vector) -> copy.put(seg, new IracEntryVersion(vector)));
      return copy;
   }

   private Map<String, TopologyIracVersion> generateNewVectorFunction(Integer s,
         Map<String, TopologyIracVersion> versions) {
      if (versions == null) {
         return Collections.singletonMap(localSite, TopologyIracVersion.newVersion(topologyId));
      } else {
         Map<String, TopologyIracVersion> copy = new HashMap<>(versions);
         copy.compute(localSite, this::incrementVersionFunction);
         return copy;
      }
   }

   private Map<String, TopologyIracVersion> getVectorFunction(Integer s,
                                                              Map<String, TopologyIracVersion> versions) {
      if (versions == null) {
         return Collections.singletonMap(localSite, TopologyIracVersion.newVersion(topologyId));
      } else {
         return versions;
      }
   }

   private TopologyIracVersion incrementVersionFunction(String site, TopologyIracVersion version) {
      return version == null ? TopologyIracVersion.newVersion(topologyId) : version.increment(topologyId);
   }

   private static Map<String, TopologyIracVersion> mergeVectorsFunction(Map<String, TopologyIracVersion> v1,
         Map<String, TopologyIracVersion> v2) {
      if (v1 == null) {
         return v2;
      } else {
         Map<String, TopologyIracVersion> copy = new HashMap<>(v1);
         for (Map.Entry<String, TopologyIracVersion> entry : v2.entrySet()) {
            copy.merge(entry.getKey(), entry.getValue(), TopologyIracVersion::max);
         }
         return copy;
      }
   }

   private Map<String, TopologyIracVersion> increment(int segment) {
      Map<String, TopologyIracVersion> result = segmentVersion.compute(segment, this::generateNewVectorFunction);
      return new HashMap<>(result);
   }

   private String scope() {
      return "___irac_version_" + cacheName;
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
         segmentVersion.compute(segment, (seg, vectorClock) -> {
            if (vectorClock == null) {
               return Collections.singletonMap(site, v);
            } else {
               Map<String, TopologyIracVersion> copy = new HashMap<>(vectorClock);
               copy.merge(site, v, TopologyIracVersion::max);
               return copy;
            }
         });
      });
   }

   private ScopedPersistentState writeState() {
      ScopedPersistentStateImpl state = new ScopedPersistentStateImpl(scope());
      state.setProperty(GlobalStateManagerImpl.VERSION, Version.getVersion());
      segmentVersion.forEach((segment, vector) ->
            vector.forEach((site, version) ->
                  state.setProperty(Integer.toString(segment) + '_' + site, version.toString())));
      return state;
   }
}
