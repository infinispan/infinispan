package org.infinispan.server.resp.commands.cluster;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

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
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.transport.NettyTransport;
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

public class SLOTS extends RespCommand implements Resp3Command {

   @GuardedBy("this")
   private CompletionStage<CharSequence> lastExecution = null;

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

      return handler.stageToReturn(lastExecution, ctx, ByteBufferUtils::stringToByteBuf);
   }

   private static CompletionStage<CharSequence> getSlotsInformation(Resp3Handler handler, ConsistentHash hash) {
      return requestNodesNetworkInformation(hash.getMembers(), handler)
            .thenApply(information -> {
               StringBuilder builder = new StringBuilder();

               Map<List<Address>, IntSet> segmentOwners = new HashMap<>();
               for (int i = 0; i < hash.getNumSegments(); i++) {
                  segmentOwners.computeIfAbsent(hash.locateOwnersForSegment(i), ignore -> IntSets.mutableEmptySet())
                        .add(i);
               }

               int size = 0;
               for (Map.Entry<List<Address>, IntSet> entry : segmentOwners.entrySet()) {
                  List<Address> owners = entry.getKey();
                  IntSet segments = entry.getValue();

                  for (int i = segments.nextSetBit(0); i >= 0; i = segments.nextSetBit(i + 1)) {
                     int runStart = i;
                     while (segments.contains(i + 1)) {
                        i++;
                     }

                     size++;
                     builder.append('*').append(2 + owners.size()).append(CRLF_STRING);
                     builder.append(':').append(runStart).append(CRLF_STRING);
                     builder.append(':').append(i).append(CRLF_STRING);
                     for (Address owner: owners) {
                        String info = information.get(owner);
                        builder.append(info);
                     }
                  }
               }
               return "*" + size + CRLF_STRING + builder;
            });
   }

   private static CompletionStage<Map<Address, String>> requestNodesNetworkInformation(List<Address> members, Resp3Handler handler) {
      Map<Address, String> responses = new ConcurrentHashMap<>(members.size());
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

   private static String readLocalInformation(String serverName, EmbeddedCacheManager ecm) {
      StringBuilder sb = new StringBuilder();
      ComponentRef<RespServer> ref = SecurityActions.getGlobalComponentRegistry(ecm)
            .getComponent(BasicComponentRegistry.class)
            .getComponent(serverName, RespServer.class);

      if (ref == null) {
         // Handle with basic information.
         return "$-1\r\n";
      }

      // An array with network information.
      sb.append("*4\r\n");
      RespServer server = ref.running();
      CacheManagerInfo info = ecm.getCacheManagerInfo();

      sb.append('$').append(server.getHost().length()).append(CRLF_STRING).append(server.getHost()).append(CRLF_STRING);
      sb.append(':').append(server.getPort()).append(CRLF_STRING);
      sb.append('$').append(info.getNodeName().length()).append(CRLF_STRING).append(info.getNodeName()).append(CRLF_STRING);

      // The last element is for additional metadata. For example, hostnames or something like that.
      NettyTransport transport = server.getTransport();
      if (transport != null) {
         String host = transport.getHostName();
         sb.append("*1\r\n").append('$').append(host.length()).append(CRLF_STRING).append(host).append(CRLF_STRING);
      } else {
         sb.append("$-1\r\n");
      }
      return sb.toString();
   }
}
