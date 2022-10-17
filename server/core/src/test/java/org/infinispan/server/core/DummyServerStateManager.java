package org.infinispan.server.core;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.server.core.transport.IpSubnetFilterRule;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class DummyServerStateManager implements ServerStateManager {
   Set<String> ignoredCaches = ConcurrentHashMap.newKeySet();

   @Override
   public CompletableFuture<Void> unignoreCache(String cacheName) {
      ignoredCaches.remove(cacheName);
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public CompletableFuture<Void> ignoreCache(String cacheName) {
      ignoredCaches.add(cacheName);
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public boolean isCacheIgnored(String cache) {
      return ignoredCaches.contains(cache);
   }

   @Override
   public Set<String> getIgnoredCaches() {
      return ignoredCaches;
   }

   @Override
   public CompletableFuture<Boolean> connectorStatus(String name) {
      return CompletableFuture.completedFuture(true);
   }

   @Override
   public CompletableFuture<Boolean> connectorStart(String name) {
      return CompletableFuture.completedFuture(true);
   }

   @Override
   public CompletableFuture<Void> connectorStop(String name) {
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public CompletableFuture<Void> setConnectorIpFilterRule(String name, Collection<IpSubnetFilterRule> filterRule) {
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public CompletableFuture<Void> clearConnectorIpFilterRules(String name) {
      return CompletableFuture.completedFuture(null);
   }

   @Override
   public ServerManagement managedServer() {
      return null;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }
}
