package org.infinispan.server.core.transport;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.management.JMException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;

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
      this.isGlobalStatsEnabled = cacheManager != null && SecurityActions.getCacheManagerConfiguration(cacheManager).globalJmxStatistics().enabled();
      this.acceptedChannels = acceptedChannels;
      this.threadNamePrefix = threadNamePrefix;
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
         CacheManagerJmxRegistration jmxRegistration = SecurityActions.getGlobalComponentRegistry(embeddedCacheManager)
               .getComponent(CacheManagerJmxRegistration.class);
         try {
            ObjectName transportNamePattern = new ObjectName(jmxRegistration.getDomain() + ":type=Server,component=Transport,name=*");
            Set<ObjectName> objectNames = jmxRegistration.getMBeanServer().queryNames(transportNamePattern, null);

            // sum the NumberOfLocalConnections from all transport MBeans that match the pattern
            int total = 0;
            for (ObjectName name : objectNames) {
               if (name.getKeyProperty("name").startsWith(serverName)) {
                  Integer connections = (Integer) jmxRegistration.getMBeanServer().getAttribute(name, "NumberOfLocalConnections");
                  total += connections;
               }
            }
            return total;
         } catch (JMException e) {
            throw new RuntimeException(e);
         }
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
