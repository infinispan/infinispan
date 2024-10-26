package org.infinispan.server.resp.commands.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.topology.CacheTopology;

import io.netty.channel.ChannelHandlerContext;
import net.jcip.annotations.GuardedBy;

/**
 * CLUSTER SLOTS
 * <p>
 * As of Redis version 7.0.0, this command is regarded as deprecated, and {@link SHARDS} is the recommended alternative.
 * This command retrieves information about the slot distribution in the cluster. Also including connection information
 * of all members.
 * </p>
 *
 * @author José Bolina
 * @see SHARDS
 * @see <a href="https://redis.io/commands/cluster-slots/">CLUSTER SLOTS</a>
 * @since 15.0
 */
public class SLOTS extends RespCommand implements Resp3Command {

   private static final BiConsumer<List<SlotInformation>, ByteBufPool> SERIALIZER = (res, alloc) -> {
      ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, res.size(), alloc);
      for (SlotInformation si : res) {
         Resp3Response.write(si, alloc, si);
      }
   };

   @GuardedBy("this")
   private CompletionStage<List<SlotInformation>> lastExecution = null;

   @GuardedBy("this")
   private ConsistentHash lastAcceptedHash = null;

   public SLOTS() {
      super(2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      AdvancedCache<?, ?> respCache = handler.cache();
      DistributionManager dm = respCache.getDistributionManager();
      if (dm == null) {
         RespErrorUtil.customError("This instance has cluster support disabled", handler.allocator());
         return handler.myStage();
      }

      CacheTopology topology = dm.getCacheTopology();
      ConsistentHash hash = topology.getCurrentCH();

      if (hash == null) {
         RespErrorUtil.customError("No consistent hash available", handler.allocator());
         return handler.myStage();
      }

      synchronized (this) {
         if (!hash.equals(lastAcceptedHash)) {
            lastExecution = getSlotsInformation(handler, hash);
            lastAcceptedHash = hash;
         }
      }

      return handler.stageToReturn(lastExecution, ctx, SERIALIZER);
   }

   private static CompletionStage<List<SlotInformation>> getSlotsInformation(Resp3Handler handler, ConsistentHash hash) {
      return requestNodesNetworkInformation(hash.getMembers(), handler)
            .thenApply(information -> {
               List<SlotInformation> response = new ArrayList<>();

               SegmentSlotRelation ssr = handler.respServer().segmentSlotRelation();

               // These two will always be initialized in first loop below
               int previousOwnedSegment = -1;
               List<Address> ownersForSegment = null;

               int slotWidth = ssr.slotWidth();
               int totalSegmentCount = hash.getNumSegments();
               for (int i = 0; i < totalSegmentCount; ++i) {
                  List<Address> currentOwners = hash.locateOwnersForSegment(i);

                  if (!currentOwners.equals(ownersForSegment) || i == (totalSegmentCount - 1)) {
                     if (ownersForSegment != null) {
                        int start = previousOwnedSegment * slotWidth;
                        int end = (i * slotWidth) - 1;
                        List<NodeInformation> nodes = new ArrayList<>();
                        for (Address owner: ownersForSegment) {
                           nodes.add(NodeInformation.create(information.get(owner)));
                        }
                        response.add(new SlotInformation(start, end, nodes));
                     }
                     ownersForSegment = currentOwners;
                     previousOwnedSegment = i;
                  }
               }
               return response;
            });
   }

   private static CompletionStage<Map<Address, List<Object>>> requestNodesNetworkInformation(List<Address> members, Resp3Handler handler) {
      Map<Address, List<Object>> responses = new ConcurrentHashMap<>(members.size());
      ClusterExecutor executor = SecurityActions.getClusterExecutor(handler.cache());
      String name = handler.respServer().getQualifiedName();
      return executor.filterTargets(members)
            .submitConsumer(e -> readLocalInformation(name, e), (address, res, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }
               responses.put(address, res);
            }).thenApply(ignore -> responses);
   }

   private static List<Object> readLocalInformation(String serverName, EmbeddedCacheManager ecm) {
      List<Object> response = new ArrayList<>();
      ComponentRef<RespServer> ref = SecurityActions.getGlobalComponentRegistry(ecm)
            .getComponent(BasicComponentRegistry.class)
            .getComponent(serverName, RespServer.class);

      if (ref == null) {
         // Handle with basic information.
         return null;
      }

      // An array with network information.
      RespServer server = ref.running();
      CacheManagerInfo info = ecm.getCacheManagerInfo();

      response.add(server.getHost());
      response.add(server.getPort());
      response.add(info.getNodeName());

      // The last element is for additional metadata. For example, hostnames or something like that.
      NettyTransport transport = server.getTransport();
      if (transport != null) {
         String host = transport.getHostName();
         response.add(List.of(host));
      } else {
         response.add(Collections.emptyList());
      }
      return response;
   }

   private record SlotInformation(int start, int end, List<NodeInformation> info) implements JavaObjectSerializer<SlotInformation> {

      @Override
      public void accept(SlotInformation si, ByteBufPool alloc) {
         int size = 2 + si.info.size();
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, size, alloc);

         Resp3Response.integers(si.start(), alloc);
         Resp3Response.integers(si.end(), alloc);
         for (NodeInformation ni : si.info) {
            Resp3Response.write(ni, alloc, ni);
         }
      }
   }

   private record NodeInformation(String host, Integer port, String name, List<String> metadata) implements JavaObjectSerializer<NodeInformation> {
      @Override
      public void accept(NodeInformation ignore, ByteBufPool alloc) {
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 4, alloc);
         Resp3Response.string(host, alloc);
         Resp3Response.integers(port, alloc);
         Resp3Response.string(name, alloc);
         Resp3Response.array(metadata, alloc, Resp3Type.BULK_STRING);
      }

      private static NodeInformation create(List<Object> values) {
         return new NodeInformation(
               (String) values.get(0),
               (Integer) values.get(1),
               (String) values.get(2),
               (List<String>) values.get(3)
         );
      }
   }
}
