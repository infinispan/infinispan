package org.infinispan.client.hotrod.impl;

import java.net.SocketAddress;

import org.infinispan.client.hotrod.RemoteCache;

import io.netty.channel.Channel;

public final class InvalidatedNearRemoteCacheTestingUtil {

   private InvalidatedNearRemoteCacheTestingUtil() {
   }

   /** Reads the key and reports whether the public statistics record a near-cache hit. */
   public static <K, V> boolean isNearCacheHit(RemoteCache<K, V> cache, K key) {
      var statistics = cache.clientStatistics();
      long hits = statistics.getNearCacheHits();
      cache.get(key);
      return statistics.getNearCacheHits() == hits + 1;
   }

   /** Returns the current transport channel, regardless of whether it has a near-cache listener. */
   public static Channel getServerChannel(InternalRemoteCache<?, ?> cache, SocketAddress server) {
      var connection = cache.getDispatcher().getHandlerForAddress(server);
      return connection == null ? null : connection.getChannel();
   }
}
