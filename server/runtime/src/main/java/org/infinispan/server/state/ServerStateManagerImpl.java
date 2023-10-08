package org.infinispan.server.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.Server;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.server.core.transport.CompositeChannelMatcher;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.core.transport.IpFilterRuleChannelMatcher;
import org.infinispan.server.core.transport.IpSubnetFilterRule;
import org.infinispan.server.core.transport.Transport;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ssl.SslHandler;

/**
 * Manages cluster-wide server state for a given {@link EmbeddedCacheManager}. This handles:
 * <ul>
 * <li>ignored caches</li>
 * <li>protocol servers</li>
 * <li>ip filters</li>
 * </ul>
 *
 * @since 12.1
 */
public final class ServerStateManagerImpl implements ServerStateManager {
   private static final String CONNECTOR_STATE_SCOPE = "connector-state";
   private static final String CONNECTOR_IPFILTER_SCOPE = "connector-ipfilter";
   private static final ScopedState IGNORED_CACHES_KEY = new ScopedState("ignored-caches", "ignored-caches");

   private final EmbeddedCacheManager cacheManager;
   private final Server server;
   private final Cache<ScopedState, Object> cache;
   private final IgnoredCaches ignored = new IgnoredCaches();
   private volatile boolean hasIgnores;

   public ServerStateManagerImpl(Server server, EmbeddedCacheManager cacheManager, GlobalConfigurationManager configurationManager) {
      this.server = server;
      this.cacheManager = cacheManager;
      this.cache = configurationManager.getStateCache();
   }

   @Override
   public void start() {
      updateLocalIgnoredCaches((IgnoredCaches) cache.get(IGNORED_CACHES_KEY));

      // Register the listeners which will react on
      cache.addListener(new IgnoredCachesListener(), new ScopeFilter(IGNORED_CACHES_KEY.getScope()), null);
      cache.addListener(new ConnectorStateListener(), new ScopeFilter(CONNECTOR_STATE_SCOPE), null);
      cache.addListener(new ConnectorIpFilterListener(), new ScopeFilter(CONNECTOR_IPFILTER_SCOPE), null);
   }

   @Override
   public void stop() {
   }

   @Override
   public CompletableFuture<Void> unignoreCache(String cacheName) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      synchronized (this) {
         ignored.caches.remove(cacheName);
         hasIgnores = !ignored.caches.isEmpty();
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);
      }
   }

   @Override
   public CompletableFuture<Void> ignoreCache(String cacheName) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      synchronized (this) {
         ignored.caches.add(cacheName);
         hasIgnores = true;
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);
      }
   }

   @Override
   public Set<String> getIgnoredCaches() {
      return Collections.unmodifiableSet(ignored.caches);
   }

   @Override
   public boolean isCacheIgnored(String cacheName) {
      return hasIgnores && ignored.caches.contains(cacheName);
   }

   @Override
   public CompletableFuture<Boolean> connectorStart(String name) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      return cache.removeAsync(new ScopedState(CONNECTOR_STATE_SCOPE, name), true).thenApply(v -> null);
   }

   @Override
   public CompletableFuture<Void> connectorStop(String name) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      return cache.putAsync(new ScopedState(CONNECTOR_STATE_SCOPE, name), true).thenApply(v -> null);
   }

   @Override
   public CompletableFuture<Boolean> connectorStatus(String name) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      return cache.containsKeyAsync(new ScopedState(CONNECTOR_STATE_SCOPE, name)).thenApply(v -> !v);
   }

   @Override
   public CompletableFuture<Void> setConnectorIpFilterRule(String name, Collection<IpSubnetFilterRule> filterRule) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      IpFilterRules ipFilterRules = new IpFilterRules();
      filterRule.forEach(r -> ipFilterRules.rules.add(new IpFilterRule(r)));
      return cache.putAsync(new ScopedState(CONNECTOR_IPFILTER_SCOPE, name), ipFilterRules).thenApply(v -> null);
   }

   @Override
   public CompletableFuture<Void> clearConnectorIpFilterRules(String name) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      return cache.putAsync(new ScopedState(CONNECTOR_IPFILTER_SCOPE, name), new IpFilterRules()).thenApply(v -> null);
   }

   @Override
   public CompletableFuture<Json> listConnections() {
      Json r = Json.array();
      for (Map.Entry<String, ProtocolServer> ps : server.getProtocolServers().entrySet()) {
         Transport transport = ps.getValue().getTransport();
         if (transport != null) {
            ChannelGroup channels = transport.getAcceptedChannels();
            channels.forEach(ch -> {
               ConnectionMetadata metadata = ConnectionMetadata.getInstance(ch);
               Json o = Json.object();
               o.set("id", metadata.id());
               o.set("server-node-name", server.getCacheManager().getAddress().toString());
               o.set("name", metadata.clientName());
               o.set("created", metadata.created());
               o.set("principal", Security.getSubjectUserPrincipalName(metadata.subject()));
               o.set("local-address", metadata.localAddress().toString());
               o.set("remote-address", metadata.remoteAddress().toString());
               o.set("protocol-version", metadata.protocolVersion());
               o.set("client-library", metadata.clientLibraryName());
               o.set("client-version", metadata.clientLibraryVersion());
               SslHandler ssl = ch.pipeline().get(SslHandler.class);
               if (ssl != null) {
                  o.set("ssl-application-protocol", ssl.applicationProtocol());
                  o.set("ssl-cipher-suite", ssl.engine().getSession().getCipherSuite());
                  o.set("ssl-protocol", ssl.engine().getSession().getProtocol());
               }
               r.add(o);
            });
         }
      }
      return CompletableFuture.completedFuture(r);
   }

   @Override
   public Json clientsReport() {
      Json result = Json.object();
      for (ProtocolServer protocolServer : server.getProtocolServers().values()) {
         Transport transport = protocolServer.getTransport();
         if (transport == null) {
            continue;
         }
         ChannelGroup channels = transport.getAcceptedChannels();
         if (channels.isEmpty()) {
            continue;
         }

         HashSet<String> clientNames = new HashSet<>();
         HashSet<String> protocolVersions = new HashSet<>();
         HashSet<String> clientLibraryNames = new HashSet<>();
         HashSet<String> clientLibraryVersions = new HashSet<>();

         for (Channel channel : channels) {
            ConnectionMetadata metadata = ConnectionMetadata.getInstance(channel);
            String clientName = metadata.clientName();
            if (clientName != null) {
               clientNames.add(clientName);
            }
            String protocolVersion = metadata.protocolVersion();
            if (protocolVersion != null) {
               protocolVersions.add(protocolVersion);
            }
            String clientLibraryName = metadata.clientLibraryName();
            if (clientLibraryName != null) {
               clientLibraryNames.add(clientLibraryName);
            }
            String clientLibraryVersion = metadata.clientLibraryVersion();
            if (clientLibraryVersion != null) {
               clientLibraryVersions.add(clientLibraryVersion);
            }
         }

         Json protocolServerReport = Json.object();
         if (!clientNames.isEmpty()){
            protocolServerReport.set("client-names", Json.make(clientNames));
         }
         if (!protocolVersions.isEmpty()){
            protocolServerReport.set("protocol-versions", Json.make(protocolVersions));
         }
         if (!clientLibraryNames.isEmpty()){
            protocolServerReport.set("client-library-names", Json.make(clientLibraryNames));
         }
         if (!clientLibraryVersions.isEmpty()){
            protocolServerReport.set("client-library-versions", Json.make(clientLibraryVersions));
         }
         result.set(protocolServer.getName(), protocolServerReport);
      }
      return result;
   }

   @Override
   public ServerManagement managedServer() {
      return server;
   }

   private void updateLocalIgnoredCaches(IgnoredCaches ignored) {
      if (ignored != null) {
         synchronized (this) {
            this.ignored.caches.clear();
            this.ignored.caches.addAll(ignored.caches);
            hasIgnores = !this.ignored.caches.isEmpty();
         }
      }
   }

   private CompletionStage<Void> updateIpFilters(String connector, Collection<IpFilterRule> rules) {
      ProtocolServer<?> protocolServer = server.getProtocolServers().get(connector);
      if (rules.isEmpty()) {
         protocolServer.getConfiguration().ipFilter().rules(Collections.emptyList());
         Server.log.connectorIpFilterCleared(connector);
         return CompletableFutures.completedNull();
      } else {
         List<IpSubnetFilterRule> localRules = new ArrayList<>(rules.size());
         for (IpFilterRule rule : rules) {
            localRules.add(new IpSubnetFilterRule(rule.cidr, IpFilterRuleType.valueOf(rule.type)));
         }
         protocolServer.getConfiguration().ipFilter().rules(localRules);
         Transport transport = getTransport(protocolServer);
         CompositeChannelMatcher matcher = new CompositeChannelMatcher(protocolServer.getChannelMatcher(), new IpFilterRuleChannelMatcher(localRules));
         return transport.closeChannels(matcher).thenApply(v -> {
            Server.log.connectorIpFilterSet(connector, localRules);
            return v;
         });
      }
   }

   @Listener(observation = Listener.Observation.POST, includeCurrentState = true, clustered = true)
   private final class IgnoredCachesListener {
      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<ScopedState, IgnoredCaches> e) {
         if (!e.isOriginLocal()) {
            updateLocalIgnoredCaches(e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<ScopedState, IgnoredCaches> e) {
         if (!e.isOriginLocal()) {
            updateLocalIgnoredCaches(e.getValue());
         }
      }
   }

   @Listener(observation = Listener.Observation.POST, includeCurrentState = true, clustered = true)
   private final class ConnectorStateListener {
      @CacheEntryCreated
      public CompletionStage<Void> created(CacheEntryCreatedEvent<ScopedState, Boolean> e) {
         // stop the connector
         String connector = e.getKey().getName();
         ProtocolServer protocolServer = server.getProtocolServers().get(connector);
         protocolServer.getConfiguration().disable();
         // Close all active connections
         Transport transport = getTransport(protocolServer);
         return transport.closeChannels(protocolServer.getChannelMatcher()).thenApply(v -> {
            Server.log.connectorStopped(connector);
            return v;
         });
      }

      @CacheEntryRemoved
      public void removed(CacheEntryRemovedEvent<ScopedState, Boolean> e) {
         // start the connector
         String connector = e.getKey().getName();
         server.getProtocolServers().get(connector).getConfiguration().enable();
         Server.log.connectorStarted(connector);
      }
   }

   private Transport getTransport(ProtocolServer protocolServer) {
      Transport transport = protocolServer.getTransport();
      if (transport == null) {
         transport = protocolServer.getEnclosingProtocolServer().getTransport();
      }
      return transport;
   }

   @Listener(observation = Listener.Observation.POST, includeCurrentState = true, clustered = true)
   private final class ConnectorIpFilterListener {
      @CacheEntryCreated
      @CacheEntryModified
      public CompletionStage<Void> modified(CacheEntryEvent<ScopedState, IpFilterRules> e) {
         return updateIpFilters(e.getKey().getName(), e.getValue().rules);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.IGNORED_CACHES)
   public static final class IgnoredCaches {

      @ProtoField(number = 1, collectionImplementation = HashSet.class)
      final Set<String> caches;

      IgnoredCaches() {
         this(ConcurrentHashMap.newKeySet());
      }

      @ProtoFactory
      IgnoredCaches(Set<String> caches) {
         // ProtoStream cannot use KeySetView directly as it does not have a zero args constructor
         this.caches = ConcurrentHashMap.newKeySet(caches.size());
         this.caches.addAll(caches);
      }

      @Override
      public String toString() {
         return "IgnoredCaches" + caches;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.IP_FILTER_RULE)
   public static final class IpFilterRule {

      @ProtoField(number = 1)
      final String cidr;

      @ProtoField(number = 2)
      final String type;

      @ProtoFactory
      IpFilterRule(String cidr, String type) {
         this.cidr = cidr;
         this.type = type;
      }

      IpFilterRule(IpSubnetFilterRule filterRule) {
         this(filterRule.cidr(), filterRule.ruleType().name());
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         IpFilterRule that = (IpFilterRule) o;
         return cidr.equals(that.cidr) && type.equals(that.type);
      }

      @Override
      public int hashCode() {
         return Objects.hash(cidr, type);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.IP_FILTER_RULES)
   public static final class IpFilterRules {

      @ProtoField(number = 1, collectionImplementation = LinkedHashSet.class)
      final Set<IpFilterRule> rules;

      IpFilterRules() {
         this(new LinkedHashSet<>());
      }

      @ProtoFactory
      IpFilterRules(Set<IpFilterRule> rules) {
         this.rules = new LinkedHashSet<>(rules.size());
         this.rules.addAll(rules);
      }
   }
}
