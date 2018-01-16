package org.infinispan.server.core.transport;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.jmx.JmxUtil;
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
         org.infinispan.remoting.transport.Transport transport = cacheManager.getTransport();
         return transport != null && transport.getMembers().size() > 1;
      }
      return false;
   }

   private int calculateGlobalConnections() {
      Cache<Object, Object> cache = cacheManager.getCache();
      DistributedExecutorService exec = new DefaultExecutorService(cache);
      try {
         // Submit calculation task
         List<CompletableFuture<Integer>> results = exec.submitEverywhere(
               new ConnectionAdderTask(threadNamePrefix));
         // Take all results and add them up with a bit of functional programming magic :)
         return results.stream().mapToInt(f -> {
            try {
               return f.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
               throw new CacheException(e);
            }
         }).sum();
      } finally {
         exec.shutdown();
      }
   }

   static class ConnectionAdderTask implements Serializable, DistributedCallable<Object, Object, Integer> {
      private final String serverName;

      Cache<Object, Object> cache;

      ConnectionAdderTask(String serverName) {
         this.serverName = serverName;
      }

      @Override
      public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
         this.cache = cache;
      }

      @Override
      public Integer call() throws Exception {
         GlobalConfiguration globalCfg = cache.getCacheManager().getCacheManagerConfiguration();
         String jmxDomain = globalCfg.globalJmxStatistics().domain();
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
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
