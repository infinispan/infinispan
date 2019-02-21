package org.infinispan.server.core.transport;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
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
      this.isGlobalStatsEnabled = cacheManager != null && cacheManager.getCacheManagerConfiguration().globalJmxStatistics().enabled();
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
      CompletableFuture<Void> results = cacheManager.executor().submitConsumer(
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

   static class ConnectionAdderTask implements Serializable, Function<EmbeddedCacheManager, Integer> {
      private final String serverName;

      ConnectionAdderTask(String serverName) {
         this.serverName = serverName;
      }

      @Override
      public Integer apply(EmbeddedCacheManager embeddedCacheManager) {
         GlobalJmxStatisticsConfiguration globalCfg = embeddedCacheManager.getCacheManagerConfiguration().globalJmxStatistics();
         String jmxDomain = globalCfg.domain();
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg.mbeanServerLookup(), globalCfg.properties());
         try {
            ObjectName transportMBeanName = new ObjectName(
                  jmxDomain + ":type=Server,component=Transport,name=" + serverName);

            return (Integer) mbeanServer.getAttribute(transportMBeanName, "NumberOfLocalConnections");
         } catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException |
               MalformedObjectNameException e) {
            throw new RuntimeException(e);
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
