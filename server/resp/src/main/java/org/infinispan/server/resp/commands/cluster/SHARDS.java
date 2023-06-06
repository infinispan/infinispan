package org.infinispan.server.resp.commands.cluster;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;
import static org.infinispan.server.resp.commands.cluster.CLUSTER.findPhysicalAddress;
import static org.infinispan.server.resp.commands.cluster.CLUSTER.findPort;
import static org.infinispan.server.resp.commands.cluster.CLUSTER.getOnlyIp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.topology.CacheTopology;

import io.netty.channel.ChannelHandlerContext;
import net.jcip.annotations.GuardedBy;

/**
 * `CLUSTER SHARDS` command.
 * <p>
 * Use the {@link CacheTopology} and retrieves information based on the {@link ConsistentHash}. We broadcast the
 * command to the current topology members to retrieve specific data from the nodes.
 *
 * @link <a href="https://redis.io/commands/cluster-shards/">CLUSTER SHARDS</a>
 * @since 15.0
 */
public class SHARDS extends RespCommand implements Resp3Command {

   @GuardedBy("this")
   private CompletionStage<CharSequence> lastExecution = null;

   @GuardedBy("this")
   private ConsistentHash lastAcceptedHash = null;

   public SHARDS() {
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
            lastExecution = readShardsInformation(hash, SecurityActions.getClusterExecutor(respCache));
            lastAcceptedHash = hash;
         }
      }
      return handler.stageToReturn(lastExecution, ctx, ByteBufferUtils::stringToByteBuf);
   }

   private static CompletionStage<CharSequence> readShardsInformation(ConsistentHash hash, ClusterExecutor executor) {
      Map<List<Address>, IntSet> segmentOwners = new HashMap<>();
      for (int i = 0; i < hash.getNumSegments(); i++) {
         segmentOwners.computeIfAbsent(hash.locateOwnersForSegment(i), ignore -> IntSets.mutableEmptySet())
               .add(i);
      }

      return readNodeInformation(hash.getMembers(), executor)
            .thenApply(information -> {
               StringBuilder response = new StringBuilder();

               // Number of elements.
               response.append('*').append(segmentOwners.size()).append(CRLF_STRING);

               for (Map.Entry<List<Address>, IntSet> entry : segmentOwners.entrySet()) {
                  List<Address> addresses = entry.getKey();
                  String leader = information.get(addresses.get(0));
                  if (leader == null) {
                     log.debugf("Not found information for leader: %s", addresses.get(0));
                     String name = addresses.get(0).toString();
                     StringBuilder sb = new StringBuilder();

                     // A health of `loading` mean that the node will be available in the future for traffic.
                     createNodeSerialized(sb, name, name, 0, "loading");
                     leader = sb.toString();
                  }

                  List<String> replicas = null;
                  if (addresses.size() > 1) {
                     replicas = new ArrayList<>();
                     for (int i = 1; i < addresses.size(); i++) {
                        String replica = information.get(addresses.get(i));
                        if (replica == null) {
                           String name = addresses.get(i).toString();
                           StringBuilder sb = new StringBuilder();
                           createNodeSerialized(sb, name, name, 0, "loading");
                           replica = sb.toString();
                        }
                        replicas.add(replica);
                     }
                  }
                  serialize(response, leader, replicas, entry.getValue());
               }
               return response;
            });
   }

   private static CompletionStage<Map<Address, String>> readNodeInformation(List<Address> members, ClusterExecutor executor) {
      final Map<Address, String> responses = new ConcurrentHashMap<>(members.size());
      return executor.filterTargets(members)
            .submitConsumer(SHARDS::readLocalNodeInformation, (address, res, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }

               responses.put(address, res);
            }).thenApply(ignore -> responses);
   }

   private static String readLocalNodeInformation(EmbeddedCacheManager ecm) {
      CacheManagerInfo manager = ecm.getCacheManagerInfo();
      String name = manager.getNodeName();
      Address address = findPhysicalAddress(ecm);
      int port = findPort(address);
      String addressString = address != null ? getOnlyIp(address) : ecm.getCacheManagerInfo().getNodeAddress();

      StringBuilder sb = new StringBuilder();
      createNodeSerialized(sb, name, addressString, port, "online");
      return sb.toString();
   }

   private static void createNodeSerialized(StringBuilder sb, String name, String address, int port, String health) {
      // Define the number of properties and values we return.
      // We do not include the role here. This is added on the caller.
      sb.append("*14\r\n");

      sb.append("$2\r\n").append("id\r\n");
      sb.append("$").append(name.length()).append(CRLF_STRING).append(name).append(CRLF_STRING);

      sb.append("$4\r\n").append("port\r\n");
      sb.append(":").append(port).append(CRLF_STRING);

      sb.append("$2\r\n").append("ip\r\n");
      sb.append("$").append(address.length()).append(CRLF_STRING).append(address).append(CRLF_STRING);

      sb.append("$8\r\n").append("endpoint\r\n");
      sb.append("$").append(address.length()).append(CRLF_STRING).append(address).append(CRLF_STRING);

      sb.append("$18\r\n").append("replication-offset\r\n");
      sb.append(":0\r\n");

      sb.append("$6\r\n").append("health\r\n");
      sb.append("$").append(health.length()).append(CRLF_STRING).append(health).append(CRLF_STRING);
   }

   private static void serialize(StringBuilder output, String leader, List<String> replicas, IntSet ranges) {
      // Each element in the list has 2 properties, the ranges and nodes, and the associated values.
      output.append("*4\r\n");

      int segmentCount = 0;
      StringBuilder segments = new StringBuilder();
      for (int i = ranges.nextSetBit(0); i >= 0; i = ranges.nextSetBit(i + 1)) {
         int runStart = i;
         while (ranges.contains(i + 1)) {
            i++;
         }

         segments.append(":").append(runStart).append(CRLF_STRING);
         segments.append(":").append(i).append(CRLF_STRING);
         segmentCount++;
      }

      // Serializing the ranges.
      output.append("$5\r\n").append("slots\r\n");
      output.append("*").append(segmentCount * 2).append(CRLF_STRING).append(segments);

      // Now serialize information about the nodes.
      // We start with the segment owner.
      output.append("$5\r\n").append("nodes\r\n");
      output.append("*").append(replicas == null || replicas.isEmpty() ? 1 : replicas.size() + 1).append(CRLF_STRING);

      if (leader != null) {
         output.append(leader).append("$4\r\nrole\r\n$6\r\nmaster\r\n");
      } else {
         output.append("$-1\r\n");
      }

      // Now any backups, if available.
      if (replicas != null) {
         for (String replica : replicas) {
            output.append(replica).append("$4\r\nrole\r\n$7\r\nreplica\r\n");
         }
      }
   }

}
