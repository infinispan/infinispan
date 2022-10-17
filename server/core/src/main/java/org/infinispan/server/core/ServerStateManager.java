package org.infinispan.server.core;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.server.core.transport.IpSubnetFilterRule;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ServerStateManager extends Lifecycle {
   CompletableFuture<Void> unignoreCache(String cacheName);

   CompletableFuture<Void> ignoreCache(String cacheName);

   boolean isCacheIgnored(String cache);

   Set<String> getIgnoredCaches();

   CompletableFuture<Boolean> connectorStatus(String name);

   CompletableFuture<Boolean> connectorStart(String name);

   CompletableFuture<Void> connectorStop(String name);

   CompletableFuture<Void> setConnectorIpFilterRule(String name, Collection<IpSubnetFilterRule> filterRule);

   CompletableFuture<Void> clearConnectorIpFilterRules(String name);

   ServerManagement managedServer();
}
