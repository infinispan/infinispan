package org.infinispan.client.hotrod.test;

import io.netty.channel.ChannelException;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.distribution.DistributionTestHelper.getFirstOwner;
import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;

/**
 * Utility methods for the Hot Rod client
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class HotRodClientTestingUtil {

   private static final Log log = LogFactory.getLog(HotRodClientTestingUtil.class, Log.class);

   /**
    * This needs to be different than the one used in the server tests in order to make sure that there's no clash.
    */
   private static final AtomicInteger uniquePort = new AtomicInteger(15232);

   public static HotRodServer startHotRodServer(EmbeddedCacheManager cacheManager, HotRodServerConfigurationBuilder builder) {
      // TODO: This is very rudimentary!! HotRodTestingUtil needs a more robust solution where ports are generated randomly and retries if already bound
      HotRodServer server = null;
      int maxTries = 10;
      int currentTries = 0;
      ChannelException lastException = null;
      while (server == null && currentTries < maxTries) {
         try {
            server = HotRodTestingUtil.startHotRodServer(cacheManager, uniquePort.incrementAndGet(), builder);
         } catch (ChannelException e) {
            if (!(e.getCause() instanceof BindException)) {
               throw e;
            } else {
               log.debug("Address already in use: [" + e.getMessage() + "], so let's try next port");
               currentTries++;
               lastException = e;
            }
         }
      }
      if (server == null && lastException != null)
         throw lastException;

      return server;
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager cacheManager) {
      return startHotRodServer(cacheManager, new HotRodServerConfigurationBuilder());
   }

   /**
    * Kills a remote cache manager.
    *
    * @param rcm the remote cache manager instance to kill
    */
   public static void killRemoteCacheManager(RemoteCacheManager rcm) {
      try {
         if (rcm != null) rcm.stop();
      } catch (Throwable t) {
         log.warn("Error stopping remote cache manager", t);
      }
   }

   /**
    * Kills a group of remote cache managers.
    *
    * @param rcm
    *           the remote cache manager instances to kill
    */
   public static void killRemoteCacheManagers(RemoteCacheManager... rcms) {
      if (rcms != null) {
         for (RemoteCacheManager rcm : rcms) {
            try {
               if (rcm != null)
                  rcm.stop();
            } catch (Throwable t) {
               log.warn("Error stopping remote cache manager", t);
            }
         }
      }

   }

   /**
    * Kills a group of Hot Rod servers.
    *
    * @param servers the group of Hot Rod servers to kill
    */
   public static void killServers(HotRodServer... servers) {
      if (servers != null) {
         for (HotRodServer server : servers) {
            try {
               if (server != null) server.stop();
            } catch (Throwable t) {
               log.warn("Error stopping Hot Rod server", t);
            }
         }
      }
   }

   /**
    * Invoke a task using a remote cache manager. This method guarantees that
    * the remote manager used in the task will be cleaned up after the task has
    * completed, regardless of the task outcome.
    *
    * @param c task to execute
    * @throws Exception if the task fails somehow
    */
   public static void withRemoteCacheManager(RemoteCacheManagerCallable c) {
      try {
         c.call();
      } finally {
         killRemoteCacheManager(c.rcm);
      }
   }

   public static <K, V> void withClientListener(Object listener, RemoteCacheManagerCallable c) {
      RemoteCache<K, V> cache = c.rcm.getCache();
      cache.addClientListener(listener);
      try {
         c.call();
      } finally {
         cache.removeClientListener(listener);
      }
   }

   public static <K, V> void withClientListener(Object listener,
         Object[] filterFactoryParams, Object[] converterFactoryParams, RemoteCacheManagerCallable c) {
      RemoteCache<K, V> cache = c.rcm.getCache();
      cache.addClientListener(listener, filterFactoryParams, converterFactoryParams);
      try {
         c.call();
      } finally {
         cache.removeClientListener(listener);
      }
   }

   public static <K> long entryVersion(Cache<byte[], ?> cache, K key) {
      byte[] lookupKey;
      try {
         lookupKey = toBytes(key);
      } catch (Exception e) {
         throw new AssertionError(e);
      }

      Metadata meta = cache.getAdvancedCache().getCacheEntry(lookupKey).getMetadata();
      return ((NumericVersion) meta.version()).getVersion();
   }

   public static byte[] toBytes(Object key) {
      try {
         return new GenericJBossMarshaller().objectToByteBuffer(key);
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   public static String getServersString(HotRodServer... servers) {
      StringBuilder builder = new StringBuilder();
      for (HotRodServer server : servers) {
         builder.append("localhost").append(':').append(server.getPort()).append(";");
      }
      return builder.toString();
   }

   public static RemoteCacheManager getRemoteCacheManager(HotRodServer server) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer()
            .host(server.getHost())
            .port(server.getPort());
      return new InternalRemoteCacheManager(builder.build());

   }

   public static byte[] getKeyForServer(HotRodServer primaryOwner) {
      return getKeyForServer(primaryOwner, null);
   }

   public static byte[] getKeyForServer(HotRodServer primaryOwner, String cacheName) {
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      Cache<?, ?> cache = cacheName != null
            ? primaryOwner.getCacheManager().getCache(cacheName)
            : primaryOwner.getCacheManager().getCache();
      Random r = new Random();
      byte[] dummy = new byte[8];
      int attemptsLeft = 1000;
      try {
         do {
            r.nextBytes(dummy);
            attemptsLeft--;
         } while (!isFirstOwner(cache, marshaller.objectToByteBuffer(dummy)) && attemptsLeft >= 0);
      } catch (IOException e) {
         throw new AssertionError(e);
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }

      if (attemptsLeft < 0)
         throw new IllegalStateException("Could not find any key owned by " + primaryOwner);

      log.infof("Binary key %s hashes to [cluster=%s,hotrod=%s]",
            Util.printArray(dummy, false), primaryOwner.getCacheManager().getAddress(),
            primaryOwner.getAddress());

      return dummy;
   }


   public static Integer getIntKeyForServer(HotRodServer primaryOwner) {
      return getIntKeyForServer(primaryOwner, null);
   }

   public static Integer getIntKeyForServer(HotRodServer primaryOwner, String cacheName) {
      Cache<?, ?> cache = cacheName != null
            ? primaryOwner.getCacheManager().getCache(cacheName)
            : primaryOwner.getCacheManager().getCache();
      Random r = new Random();
      byte[] dummy;
      Integer dummyInt;
      int attemptsLeft = 1000;
      do {
         dummyInt = r.nextInt();
         dummy = toBytes(dummyInt);
         attemptsLeft--;
      } while (!isFirstOwner(cache, dummy) && attemptsLeft >= 0);

      if (attemptsLeft < 0)
         throw new IllegalStateException("Could not find any key owned by " + primaryOwner);

      log.infof("Integer key %s hashes to [cluster=%s,hotrod=%s]",
            dummyInt, primaryOwner.getCacheManager().getAddress(),
            primaryOwner.getAddress());

      return dummyInt;
   }

   /**
    * Get a split-personality key, whose POJO version hashes to the primary
    * owner passed in, but it's binary version does not.
    */
   public static Integer getSplitIntKeyForServer(HotRodServer primaryOwner, HotRodServer binaryOwner, String cacheName) {
      Cache<?, ?> cache = cacheName != null
            ? primaryOwner.getCacheManager().getCache(cacheName)
            : primaryOwner.getCacheManager().getCache();

      Cache<?, ?> binaryOwnerCache = cacheName != null
            ? binaryOwner.getCacheManager().getCache(cacheName)
            : binaryOwner.getCacheManager().getCache();

      Random r = new Random();
      byte[] dummy;
      Integer dummyInt;
      int attemptsLeft = 1000;
      boolean primaryOwnerFound = false;
      boolean binaryOwnerFound = false;
      do {
         dummyInt = r.nextInt();
         dummy = toBytes(dummyInt);
         attemptsLeft--;
         primaryOwnerFound = isFirstOwner(cache, dummyInt);
         binaryOwnerFound = isFirstOwner(binaryOwnerCache, dummy);
      } while (!(primaryOwnerFound && binaryOwnerFound) && attemptsLeft >= 0);

      if (attemptsLeft < 0)
         throw new IllegalStateException("Could not find any key owned by " + primaryOwner);

      log.infof("Integer key [pojo=%s,bytes=%s] hashes to [cluster=%s,hotrod=%s], but the binary version's owner is [cluster=%s,hotrod=%s]",
            Util.toHexString(dummy), dummyInt,
            primaryOwner.getCacheManager().getAddress(), primaryOwner.getAddress(),
            binaryOwner.getCacheManager().getAddress(), binaryOwner.getAddress());

      return dummyInt;
   }

   public static <T extends FailoverRequestBalancingStrategy> T getLoadBalancer(RemoteCacheManager client) {
      TcpTransportFactory transportFactory = null;
      if (client instanceof InternalRemoteCacheManager) {
         transportFactory = (TcpTransportFactory) ((InternalRemoteCacheManager) client).getTransportFactory();
      } else {
         transportFactory = TestingUtil.extractField(client, "transportFactory");
      }
      return (T) transportFactory.getBalancer(HotRodConstants.DEFAULT_CACHE_NAME_BYTES);
   }


   public static void findServerAndKill(RemoteCacheManager client,
         Collection<HotRodServer> servers, Collection<EmbeddedCacheManager> cacheManagers) {
      InetSocketAddress addr = (InetSocketAddress) getLoadBalancer(client).nextServer(null);
      for (HotRodServer server : servers) {
         if (server.getPort() == addr.getPort()) {
            HotRodClientTestingUtil.killServers(server);
            TestingUtil.killCacheManagers(server.getCacheManager());
            cacheManagers.remove(server.getCacheManager());
            TestingUtil.blockUntilViewsReceived(50000, false, cacheManagers);
         }
      }
   }

}
