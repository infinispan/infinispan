package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.notFoundResponseFuture;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.util.JVMMemoryInfoInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Messages;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.server.core.transport.IpSubnetFilterRule;
import org.infinispan.server.core.transport.Transport;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * @since 10.0
 */
public class ServerResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private static final ServerInfo SERVER_INFO = new ServerInfo();

   public ServerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/server/").handleWith(this::info)
            .invocation().methods(GET).path("/v2/server/config").handleWith(this::config)
            .invocation().methods(GET).path("/v2/server/env").handleWith(this::env)
            .invocation().methods(GET).path("/v2/server/memory").handleWith(this::memory)
            .invocation().methods(POST).path("/v2/server/").withAction("stop").handleWith(this::stop)
            .invocation().methods(GET).path("/v2/server/threads").handleWith(this::threads)
            .invocation().methods(GET).path("/v2/server/report").handleWith(this::report)
            .invocation().methods(GET).path("/v2/server/cache-managers").handleWith(this::cacheManagers)
            .invocation().methods(GET).path("/v2/server/ignored-caches/{cache-manager}").handleWith(this::listIgnored)
            .invocation().methods(POST, DELETE).path("/v2/server/ignored-caches/{cache-manager}/{cache}").handleWith(this::doIgnoreOp)
            .invocation().methods(GET).path("/v2/server/connectors").handleWith(this::listConnectors)
            .invocation().methods(GET).path("/v2/server/connectors/{connector}").handleWith(this::connectorStatus)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}").withAction("start").handleWith(this::connectorStartStop)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}").withAction("stop").handleWith(this::connectorStartStop)
            .invocation().methods(GET).path("/v2/server/connectors/{connector}/ip-filter").handleWith(this::connectorIpFilterList)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}/ip-filter").handleWith(this::connectorIpFilterSet)
            .invocation().methods(DELETE).path("/v2/server/connectors/{connector}/ip-filter").handleWith(this::connectorIpFilterClear)
            .create();
   }

   private CompletionStage<RestResponse> doIgnoreOp(RestRequest restRequest) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder().status(NO_CONTENT);
      boolean add = restRequest.method().equals(POST);

      String cacheManagerName = restRequest.variables().get("cache-manager");
      DefaultCacheManager cacheManager = invocationHelper.getServer().getCacheManager(cacheManagerName);

      if (cacheManager == null) return completedFuture(builder.status(NOT_FOUND).build());

      String cacheName = restRequest.variables().get("cache");

      if (!cacheManager.getCacheNames().contains(cacheName)) {
         return completedFuture(builder.status(NOT_FOUND).build());
      }
      ServerManagement server = invocationHelper.getServer();
      ServerStateManager ignoreManager = server.getServerStateManager();
      return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            add ? ignoreManager.ignoreCache(cacheName).thenApply(r -> builder.build()) :
                  ignoreManager.unignoreCache(cacheName).thenApply(r -> builder.build())
      );
   }

   private CompletionStage<RestResponse> listIgnored(RestRequest restRequest) {
      String cacheManagerName = restRequest.variables().get("cache-manager");
      DefaultCacheManager cacheManager = invocationHelper.getServer().getCacheManager(cacheManagerName);

      if (cacheManager == null) return notFoundResponseFuture();
      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      Set<String> ignored = serverStateManager.getIgnoredCaches();
      return asJsonResponseFuture(Json.make(ignored));
   }

   private CompletionStage<RestResponse> cacheManagers(RestRequest restRequest) {
      return asJsonResponseFuture(Json.make(invocationHelper.getServer().cacheManagerNames()));
   }

   private CompletionStage<RestResponse> connectorStartStop(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder().status(NO_CONTENT);
      String connectorName = restRequest.variables().get("connector");
      ProtocolServer connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());
      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      switch (restRequest.getAction()) {
         case "start":
            return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
                  serverStateManager.connectorStart(connectorName).thenApply(r -> builder.build()));
         case "stop":
            if (connector.equals(invocationHelper.getProtocolServer()) || connector.equals(invocationHelper.getProtocolServer().getEnclosingProtocolServer())) {
               return completedFuture(builder.status(CONFLICT).entity(Messages.MSG.connectorMatchesRequest(connectorName)).build());
            } else {
               return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
                     serverStateManager.connectorStop(connectorName).thenApply(r -> builder.build()));
            }
      }
      return completedFuture(builder.status(BAD_REQUEST).build());
   }

   private CompletionStage<RestResponse> connectorStatus(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      String connectorName = restRequest.variables().get("connector");

      ProtocolServer connector = getProtocolServer(restRequest);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();

      Json info = Json.object()
            .set("name", connectorName)
            .set("ip-filter-rules", ipFilterRulesAsJson(connector));
      Transport transport = connector.getTransport();
      if (transport != null) {
         info.set("host", transport.getHostName())
               .set("port", transport.getPort())
               .set("local-connections", transport.getNumberOfLocalConnections())
               .set("global-connections", transport.getNumberOfGlobalConnections())
               .set("io-threads", transport.getNumberIOThreads())
               .set("pending-tasks", transport.getPendingTasks())
               .set("total-bytes-read", transport.getTotalBytesRead())
               .set("total-bytes-written", transport.getTotalBytesWritten())
               .set("send-buffer-size", transport.getSendBufferSize())
               .set("receive-buffer-size", transport.getReceiveBufferSize());
      }
      return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            serverStateManager.connectorStatus(connectorName).thenApply(b -> builder.contentType(APPLICATION_JSON).entity(info.set("enabled", b)).build()));
   }

   private CompletionStage<RestResponse> connectorIpFilterList(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();

      ProtocolServer connector = getProtocolServer(restRequest);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      return completedFuture(addEntityAsJson(ipFilterRulesAsJson(connector), builder).build());
   }

   private Json ipFilterRulesAsJson(ProtocolServer connector) {
      Collection<IpSubnetFilterRule> rules = connector.getConfiguration().ipFilter().rules();
      Json array = Json.array();
      for (IpSubnetFilterRule rule : rules) {
         array.add(Json.object().set("type", rule.ruleType().name().toLowerCase()).set("from", rule.cidr()));
      }
      return array;
   }

   private ProtocolServer getProtocolServer(RestRequest restRequest) {
      String connectorName = restRequest.variables().get("connector");
      return invocationHelper.getServer().getProtocolServers().get(connectorName);
   }

   private CompletionStage<RestResponse> connectorIpFilterClear(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder().status(NO_CONTENT);

      String connectorName = restRequest.variables().get("connector");
      ProtocolServer connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            serverStateManager.clearConnectorIpFilterRules(connectorName).thenApply(r -> builder.build()));
   }

   private CompletionStage<RestResponse> listConnectors(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      return asJsonResponseFuture(Json.make(invocationHelper.getServer().getProtocolServers().keySet()));
   }

   private CompletionStage<RestResponse> connectorIpFilterSet(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder().status(NO_CONTENT);

      String connectorName = restRequest.variables().get("connector");
      ProtocolServer connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      Json json = Json.read(restRequest.contents().asString());
      if (!json.isArray()) {
         return completedFuture(builder.status(BAD_REQUEST).build());
      }
      List<Json> list = json.asJsonList();
      List<IpSubnetFilterRule> rules = new ArrayList<>(list.size());
      for (Json o : list) {
         if (!o.has("type") || !o.has("cidr")) {
            return completedFuture(builder.status(BAD_REQUEST).build());
         } else {
            rules.add(new IpSubnetFilterRule(o.at("cidr").asString(), IpFilterRuleType.valueOf(o.at("type").asString())));
         }
      }
      // Verify that none of the REJECT rules match the address from which the request was made
      if (connector.equals(invocationHelper.getProtocolServer()) || connector.equals(invocationHelper.getProtocolServer().getEnclosingProtocolServer())) {
         InetSocketAddress remoteAddress = restRequest.getRemoteAddress();
         for (IpSubnetFilterRule rule : rules) {
            if (rule.ruleType() == IpFilterRuleType.REJECT && rule.matches(remoteAddress)) {
               return completedFuture(builder.status(CONFLICT).entity(Messages.MSG.rejectRuleMatchesRequestAddress(rule, remoteAddress)).build());
            }
         }
      }

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            serverStateManager.setConnectorIpFilterRule(connectorName, rules).thenApply(r -> builder.build()));
   }

   private CompletionStage<RestResponse> memory(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      return asJsonResponseFuture(new JVMMemoryInfoInfo().toJson());
   }

   private CompletionStage<RestResponse> env(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      return asJsonResponseFuture(Json.make(System.getProperties()));
   }

   private CompletionStage<RestResponse> info(RestRequest restRequest) {
      return asJsonResponseFuture(SERVER_INFO.toJson());
   }

   private CompletionStage<RestResponse> threads(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      return completedFuture(new NettyRestResponse.Builder()
            .contentType(TEXT_PLAIN).entity(Util.threadDump())
            .build());
   }

   private CompletionStage<RestResponse> report(RestRequest restRequest) {
      ServerManagement server = invocationHelper.getServer();
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      return Security.doAs(restRequest.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            server.getServerReport().handle((path, t) -> {
               if (t != null) {
                  return responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).build();
               } else {
                  return responseBuilder
                        .contentType(MediaType.fromString("application/gzip"))
                        .header("Content-Disposition",
                              String.format("attachment; filename=\"%s-%s-%3$tY%3$tm%3$td%3$tH%3$tM%3$tS-report.tar.gz\"",
                                    Version.getBrandName().toLowerCase().replaceAll("\\s", "-"),
                                    invocationHelper.getRestCacheManager().getNodeName(),
                                    Calendar.getInstance())
                        )
                        .entity(path.toFile()).build();
               }
            })
      );
   }

   private CompletionStage<RestResponse> stop(RestRequest restRequest) {
      Security.doAs(restRequest.getSubject(), (PrivilegedAction) () -> {
         invocationHelper.getServer().serverStop(Collections.emptyList());
         return null;
      });

      return CompletableFuture.completedFuture(new NettyRestResponse.Builder()
            .status(NO_CONTENT).build());
   }

   private CompletionStage<RestResponse> config(RestRequest restRequest) {
      SecurityActions.checkPermission(invocationHelper, restRequest, AuthorizationPermission.ADMIN);
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String json = invocationHelper.getJsonWriter().toJSON(invocationHelper.getServer().getConfiguration());
      responseBuilder.entity(json).contentType(APPLICATION_JSON);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   static class ServerInfo implements JsonSerialization {
      private static final Json json = Json.object("version", Version.printVersion());

      @Override
      public Json toJson() {
         return json;
      }
   }
}
