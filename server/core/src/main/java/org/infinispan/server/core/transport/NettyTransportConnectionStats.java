package org.infinispan.server.core.transport;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.ProtocolServer;

import io.netty.channel.group.ChannelGroup;

class NettyTransportConnectionStats {

   private final EmbeddedCacheManager cacheManager;
   private final boolean isGlobalStatsEnabled;
   private final ChannelGroup acceptedChannels;
   private final String threadNamePrefix;
   private final AtomicLong totalBytesWritten = new AtomicLong();
   private final AtomicLong totalBytesRead = new AtomicLong();

   public NettyTransportConnectionStats(EmbeddedCacheManager cacheManager, ChannelGroup acceptedChannels, String threadNamePrefix) {
      this.cacheManager = cacheManager;
      this.acceptedChannels = acceptedChannels;
      this.threadNamePrefix = threadNamePrefix;
      this.isGlobalStatsEnabled = cacheManager != null && SecurityActions.getCacheManagerConfiguration(cacheManager).statistics();
   }

   private void increment(AtomicLong base, long bytes) {
      if (isGlobalStatsEnabled)
         base.addAndGet(bytes);
   }

   public void incrementTotalBytesWritten(long bytes) {
      increment(totalBytesWritten, bytes);
   }

   public void incrementTotalBytesRead(long bytes) {
      increment(totalBytesRead, bytes);
   }

   public long getTotalBytesWritten() {
      return totalBytesWritten.get();
   }

   public long getTotalBytesRead() {
      return totalBytesRead.get();
   }

   private boolean needDistributedCalculation() {
      if (cacheManager != null) {
         return cacheManager.getMembers() != null && cacheManager.getMembers().size() > 1;
      }
      return false;
   }

   private int calculateGlobalConnections() {
      AtomicInteger connectionCount = new AtomicInteger();
      // Submit calculation task
      CompletableFuture<Void> results = SecurityActions.getClusterExecutor(cacheManager).submitConsumer(
            new ConnectionAdderTask(threadNamePrefix), (a, v, t) -> {
               if (t != null) {
                  throw new CacheException(t);
               }
               connectionCount.addAndGet(v);
            });
      // Take all results and add them up with a bit of functional programming magic :)
      try {
         results.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new CacheException(e);
      }
      return connectionCount.get();
   }

   @SerializeWith(NettyTransportConnectionStats.ConnectionAdderTask.Externalizer.class)
   static class ConnectionAdderTask implements Function<EmbeddedCacheManager, Integer> {
      private final String serverName;

      ConnectionAdderTask(String serverName) {
         this.serverName = serverName;
      }

      @Override
      public Integer apply(EmbeddedCacheManager embeddedCacheManager) {
         ProtocolServer<?> protocolServer = SecurityActions.getGlobalComponentRegistry(embeddedCacheManager)
               .getComponent(ProtocolServer.class, serverName);
         // protocol server not registered; so no connections are open.
         if (protocolServer == null) {
            return 0;
         }
         Transport transport = protocolServer.getTransport();
         // check if the transport is up; otherwise no connections are open
         return transport == null ? 0 : transport.getNumberOfLocalConnections();
      }

      public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<ConnectionAdderTask> {
         @Override
         public void writeObject(ObjectOutput output, ConnectionAdderTask task) throws IOException {
            output.writeUTF(task.serverName);
         }

         @Override
         public ConnectionAdderTask readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ConnectionAdderTask(input.readUTF());
         }
      }
   }

   public Integer getNumberOfLocalConnections() {
      return acceptedChannels.size();
   }

   public Integer getNumberOfGlobalConnections() {
      if (needDistributedCalculation()) {
         return calculateGlobalConnections();
      } else {
         return getNumberOfLocalConnections();
      }
   }
}
