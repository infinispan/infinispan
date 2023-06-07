package org.infinispan.server.resp.commands.cluster;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Instant;
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
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.topology.CacheTopology;

import io.netty.channel.ChannelHandlerContext;
import net.jcip.annotations.GuardedBy;

/**
 * `<code>CLUSTER NODES</code>` command.
 * <p>
 *  A command that uses the current {@link CacheTopology} and {@link ConsistentHash} to retrieve information about
 *  the nodes. The response slightly changes with the node executing the command, as it is necessary to identify itself.
 *  The execution is broadcast for nodes in the topology to identify themselves. So the response are cached according
 *  to the node that executed the command and the topology.
 * </p>
 *
 * @link <a href="https://redis.io/commands/cluster-nodes/">CLUSTER NODES</a>
 * @since 15.0
 * @author José Bolina
 */
public class NODES extends RespCommand implements Resp3Command {

   @GuardedBy("this")
   protected ConsistentHash hash = null;

   @GuardedBy("this")
   protected CompletionStage<CharSequence> response = null;

   public NODES() {
      super(2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      handler.checkPermission(AuthorizationPermission.ADMIN);
      AdvancedCache<?, ?> respCache = handler.cache();
      DistributionManager dm = respCache.getDistributionManager();
      if (dm == null) {
         RespErrorUtil.customError("This instance has cluster support disabled", handler.allocator());
         return handler.myStage();
      }

      CacheTopology topology = dm.getCacheTopology();
      ConsistentHash currentCH = topology.getCurrentCH();

      if (currentCH == null) {
         RespErrorUtil.customError("No consistent hash available", handler.allocator());
         return handler.myStage();
      }

      synchronized (this) {
         if (!currentCH.equals(hash)) {
            EmbeddedCacheManager ecm = SecurityActions.getEmbeddedCacheManager(respCache);
            response = requestClusterInformation(handler, ctx, ecm, topology);
            hash = currentCH;
         }
      }

      return handler.stageToReturn(response, ctx, ByteBufferUtils::stringToByteBuf);
   }

   protected static CompletionStage<CharSequence> requestClusterInformation(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                            EmbeddedCacheManager ecm, CacheTopology topology) {
      ConsistentHash hash = topology.getCurrentCH();
      return readNodeInformation(hash.getMembers(), handler)
            .thenApply(information -> {
               // The response is a bulk string, each line contains information about one node.
               // The format of each line is:
               // <id> <ip:port@cport[,hostname[,auxiliary_field=value]*]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
               // More information about each field is available at the command link.
               // Since some of that information doesn't make sense for Infinispan, so we add some simple value since it is required.
               StringBuilder response = new StringBuilder();
               Address local = ecm.getAddress();
               int cport = findClientPort(ctx.channel().remoteAddress());
               for (Address member : hash.getMembers()) {
                  boolean isMyself = member.equals(local);
                  IntSet owner = IntSets.from(hash.getPrimarySegmentsForOwner(member));
                  String initial = information.get(member);
                  String health = "connected";

                  if (initial != null) {
                     // We were able to connect the node.
                     // This contains the <id> <ip:port@cport[,hostname[,auxiliary_field=value]*]>
                     response.append(String.format(initial, cport));
                     if (isMyself) {
                        response.append("myself,");
                     }
                  } else {
                     // We could not retrieve information from the node.
                     // We add the information as if was disconnected.
                     response.append(member).append(' ');

                     if (isMyself) {
                        RespServer server = handler.respServer();
                        response.append(server.getHost()).append(':').append(server.getPort());
                        response.append('@').append(cport);
                        response.append(",,shard-id=").append(member).append(' ');
                        response.append("myself,");
                     } else {
                        response.append(":0@0 noaddr,fail?,");
                     }
                     health = "disconnected";
                  }

                  response.append("master").append(' ');
                  response.append('-').append(' ');
                  response.append('0').append(' ');
                  response.append(Instant.now().getEpochSecond()).append(' ');
                  response.append(topology.getTopologyId()).append(' ');
                  response.append(health).append(' ');
                  serializeSegments(response, owner);
                  response.append('\n');
               }
               return "$" + response.length() + CRLF_STRING + response + CRLF_STRING;
            });
   }

   private static void serializeSegments(StringBuilder response, IntSet ranges) {
      boolean first = true;
      for (int i = ranges.nextSetBit(0); i >= 0; i = ranges.nextSetBit(i + 1)) {
         if (!first) {
            response.append(' ');
         }
         first = false;
         int runStart = i;
         while (ranges.contains(i + 1)) {
            i++;
         }
         response.append(runStart).append('-').append(i);
      }
   }

   private static CompletionStage<Map<Address, String>> readNodeInformation(List<Address> members, Resp3Handler handler) {
      final Map<Address, String> response = new ConcurrentHashMap<>(members.size());
      ClusterExecutor executor = SecurityActions.getClusterExecutor(handler.cache());
      String sqn = handler.respServer().getQualifiedName();
      return executor.filterTargets(members)
            .submitConsumer(ecm -> readLocalNodeInformation(sqn, ecm), (address, res, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }
               response.put(address, res);
            }).thenApply(ignore -> response);
   }

   private static String readLocalNodeInformation(String serverName, EmbeddedCacheManager ecm) {
      CacheManagerInfo info = ecm.getCacheManagerInfo();
      ComponentRef<RespServer> ref = SecurityActions.getGlobalComponentRegistry(ecm)
            .getComponent(BasicComponentRegistry.class)
            .getComponent(serverName, RespServer.class);
      String name = info.getNodeName();

      StringBuilder sb = new StringBuilder();
      sb.append(name).append(' ');

      if (ref != null) {
         RespServer server = ref.running();
         sb.append(server.getHost()).append(':').append(server.getPort()).append('@').append("%d");
         sb.append(",,shard-id=").append(name).append(' ');
      } else {
         sb.append(":0@0 noaddr,");
      }
      return sb.toString();
   }

   private static int findClientPort(SocketAddress addr) {
      if (addr instanceof InetSocketAddress) {
         return ((InetSocketAddress) addr).getPort();
      }
      return 0;
   }

}
