package org.infinispan.server.resp.commands.cluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.NetworkAddress;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.topology.CacheTopology;

import io.netty.channel.ChannelHandlerContext;

/**
 * CLUSTER SHARDS
 * <p>
 * Use the {@link CacheTopology} and retrieves information based on the {@link ConsistentHash}. We broadcast the
 * command to the current topology members to retrieve specific data from the nodes.
 * </p>
 *
 * @see <a href="https://redis.io/commands/cluster-shards/">CLUSTER SHARDS</a>
 * @since 15.0
 */
public class SHARDS extends RespCommand implements Resp3Command {

   private static final BiConsumer<List<ShardInformation>, ResponseWriter> SERIALIZER = (res, writer) -> {
      // Write the size of the response array first.
      writer.arrayStart(res.size());

      // Proceed to serialize each element.
      for (ShardInformation si : res) {
         writer.arrayNext();
         writer.write(si, si);
      }
      writer.arrayEnd();
   };

   public SHARDS() {
      super(2, 0, 0, 0, AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      AdvancedCache<?, ?> respCache = handler.cache();
      DistributionManager dm = respCache.getDistributionManager();
      if (dm == null) {
         handler.writer().customError("This instance has cluster support disabled");
         return handler.myStage();
      }

      CacheTopology topology = dm.getCacheTopology();
      ConsistentHash hash = topology.getCurrentCH();

      if (hash == null) {
         handler.writer().customError("No consistent hash available");
         return handler.myStage();
      }

      CompletionStage<List<ShardInformation>> info = readShardsInformation(hash, SecurityActions.getClusterExecutor(respCache),
         handler.respServer().segmentSlotRelation().slotWidth(), handler.respServer().getQualifiedName(), ((InetSocketAddress) ctx.channel().localAddress()).getAddress().getAddress());


      return handler.stageToReturn(info, ctx, SERIALIZER);
   }

   private static CompletionStage<List<ShardInformation>> readShardsInformation(ConsistentHash hash, ClusterExecutor executor, int slotWidth, String serverName, byte[] localAddress) {

      Map<List<Address>, IntSet> segmentOwners = new HashMap<>();
      for (int i = 0; i < hash.getNumSegments(); i++) {
         segmentOwners.computeIfAbsent(hash.locateOwnersForSegment(i), ignore -> IntSets.mutableEmptySet(hash.getNumSegments()))
               .add(i);
      }

      return readNodeInformation(hash.getMembers(), executor, serverName, localAddress)
            .thenApply(information -> {
               List<ShardInformation> response = new ArrayList<>();

               for (Map.Entry<List<Address>, IntSet> entry : segmentOwners.entrySet()) {
                  List<Address> addresses = entry.getKey();
                  Map<String, Object> leader = information.get(addresses.get(0));
                  if (leader == null) {
                     log.debugf("Not found information for leader: %s", addresses.get(0));
                     String name = addresses.get(0).toString();

                     // A health of `loading` mean that the node will be available in the future for traffic.
                     leader = createNodeSerialized(name, name, 0, "loading");
                  }

                  List<Map<String, Object>> replicas = null;
                  if (addresses.size() > 1) {
                     replicas = new ArrayList<>();
                     for (int i = 1; i < addresses.size(); i++) {
                        Map<String, Object> replica = information.get(addresses.get(i));
                        if (replica == null) {
                           String name = addresses.get(i).toString();
                           replica = createNodeSerialized(name, name, 0, "loading");
                        }
                        replicas.add(replica);
                     }
                  }
                  response.add(serialize(leader, replicas, entry.getValue(), slotWidth));
               }
               return response;
            });
   }

   private static CompletionStage<Map<Address, Map<String, Object>>> readNodeInformation(List<Address> members, ClusterExecutor executor, String serverName, byte[] localAddress) {
      final Map<Address, Map<String, Object>> responses = new ConcurrentHashMap<>(members.size());
      return executor.filterTargets(members)
            .submitConsumer(ecm -> readLocalNodeInformation(ecm, serverName, localAddress), (address, res, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }

               responses.put(address, res);
            }).thenApply(ignore -> responses);
   }

   private static Map<String, Object> readLocalNodeInformation(EmbeddedCacheManager ecm, String serverName, byte[] localAddress) {
      CacheManagerInfo manager = ecm.getCacheManagerInfo();
      String name = manager.getNodeName();
      var address = NetworkAddress.findInterfaceAddressMatchingInetAddress(localAddress);
      String host = address == null ? ecm.getCacheManagerInfo().getNodeAddress() : address;
      ComponentRef<RespServer> ref = RespServer.fromCacheManager(ecm, serverName);
      NettyTransport transport = ref.running().getTransport();
      if (transport == null) {
         transport = (NettyTransport) ref.running().getEnclosingProtocolServer().getTransport();
      }
      int port = transport.getPort();
      return createNodeSerialized(name, host, port, "online");
   }

   private static Map<String, Object> createNodeSerialized(String name, String address, int port, String health) {
      Map<String, Object> data = new HashMap<>();

      data.put("id", name);
      data.put("port", port);
      data.put("ip", address);
      data.put("endpoint", address);
      data.put("replication-offset", 0);
      data.put("health", health);
      return data;
   }

   private static ShardInformation serialize(Map<String, Object> leader, List<Map<String, Object>> replicas, IntSet ranges, int slotWidth) {
      // Each element in the list has 2 properties, the ranges and nodes, and the associated values.
      List<Integer> segments = new ArrayList<>();
      for (int i = ranges.nextSetBit(0); i >= 0; i = ranges.nextSetBit(i + 1)) {
         int runStart = i;
         while (ranges.contains(i + 1)) {
            i++;
         }

         segments.add(runStart * slotWidth);
         int endSlot = (i - 1) * slotWidth;
         // In case if the numSegments is not divisible by 2
         if (endSlot > SegmentSlotRelation.SLOT_SIZE) {
            endSlot = SegmentSlotRelation.SLOT_SIZE - 1;
         }
         segments.add(endSlot);
      }

      List<NodeInformation> nodes = new ArrayList<>();
      if (leader != null) {
         nodes.add(NodeInformation.from(leader, "master"));
      } else {
         nodes.add(null);
      }

      // Now any backups, if available.
      if (replicas != null) {
         for (Map<String, Object> replica : replicas) {
            nodes.add(NodeInformation.from(replica, "replica"));
         }
      }

      return new ShardInformation(segments, nodes);
   }

   private record ShardInformation(List<Integer> slots, List<NodeInformation> nodes) implements JavaObjectSerializer<ShardInformation> {

      @Override
      public void accept(ShardInformation ignore, ResponseWriter writer) {
         writer.writeNumericPrefix(RespConstants.MAP, 2);

         // First key and value for slots.
         writer.string("slots");
         writer.array(slots, Resp3Type.INTEGER);

         // Key and value for nodes.
         writer.string("nodes");
         writer.arrayStart(nodes.size());
         for (NodeInformation node : nodes) {
            writer.arrayNext();
            writer.write(node, node);
         }
         writer.arrayEnd();
      }
   }

   private record NodeInformation(String id, Integer port,
                                  String ip, String endpoint,
                                  Integer offset, String health, String role) implements JavaObjectSerializer<NodeInformation> {

      private static NodeInformation from(Map<String, Object> data, String role) {
         return new NodeInformation(
               (String) data.get("id"),
               (Integer) data.get("port"),
               (String) data.get("ip"),
               (String) data.get("endpoint"),
               (Integer) data.get("replication-offset"),
               (String) data.get("health"),
               role
         );
      }

      @Override
      public void accept(NodeInformation ignore, ResponseWriter writer) {
         // Write this object as a map.
         writer.writeNumericPrefix(RespConstants.MAP, 7);

         writer.string("id");
         writer.string(id);

         writer.string("port");
         writer.integers(port);

         writer.string("ip");
         writer.string(ip);

         writer.string("endpoint");
         writer.string(endpoint);

         writer.string("replication-offset");
         writer.integers(offset);

         writer.string("health");
         writer.string(health);

         writer.string("role");
         writer.string(role);
      }
   }
}
