package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.io.StringBuilderWriter;
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
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.Messages;
import org.infinispan.security.AuditContext;
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
   private final Executor blockingExecutor;

   public ServerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.blockingExecutor = invocationHelper.getExecutor();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/server/")
            .handleWith(this::info)
            .invocation().methods(GET).path("/v2/server/config")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::config)
            .invocation().methods(GET).path("/v2/server/env")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::env)
            .invocation().methods(GET).path("/v2/server/memory")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::memory)
            .invocation().methods(POST).path("/v2/server/memory").withAction("heap-dump")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::heapDump)
            .invocation().methods(POST).path("/v2/server/").withAction("stop")
            .permission(AuthorizationPermission.ADMIN)
            .handleWith(this::stop)
            .invocation().methods(GET).path("/v2/server/threads")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::threads)
            .invocation().methods(GET).path("/v2/server/report")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER)
            .handleWith(this::report)
            .invocation().methods(GET).path("/v2/server/cache-managers")
            .handleWith(this::cacheManagers)
            .invocation().methods(GET).path("/v2/server/ignored-caches/{cache-manager}")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::listIgnored)
            .invocation().methods(POST, DELETE).path("/v2/server/ignored-caches/{cache-manager}/{cache}")
            .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER)
            .handleWith(this::doIgnoreOp)
            .invocation().methods(GET).path("/v2/server/connectors")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR LIST").auditContext(AuditContext.SERVER)
            .handleWith(this::listConnectors)
            .invocation().methods(GET).path("/v2/server/connectors/{connector}")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR GET").auditContext(AuditContext.SERVER)
            .handleWith(this::connectorStatus)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}").withAction("start")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR START").auditContext(AuditContext.SERVER)
            .handleWith(this::connectorStartStop)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}").withAction("stop")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR STOP").auditContext(AuditContext.SERVER)
            .handleWith(this::connectorStartStop)
            .invocation().methods(GET).path("/v2/server/connectors/{connector}/ip-filter")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR FILTER GET").auditContext(AuditContext.SERVER)
            .handleWith(this::connectorIpFilterList)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}/ip-filter")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR FILTER SET").auditContext(AuditContext.SERVER)
            .handleWith(this::connectorIpFilterSet)
            .invocation().methods(DELETE).path("/v2/server/connectors/{connector}/ip-filter")
            .permission(AuthorizationPermission.ADMIN).name("CONNECTOR FILTER DELETE").auditContext(AuditContext.SERVER)
            .handleWith(this::connectorIpFilterClear)
            .invocation().methods(GET).path("/v2/server/datasources")
            .permission(AuthorizationPermission.ADMIN).name("DATASOURCE LIST").auditContext(AuditContext.SERVER)
            .handleWith(this::dataSourceList)
            .invocation().methods(POST).path("/v2/server/datasources/{datasource}").withAction("test")
            .permission(AuthorizationPermission.ADMIN).name("DATASOURCE TEST").auditContext(AuditContext.SERVER)
            .handleWith(this::dataSourceTest)
            .create();
   }

   private CompletionStage<RestResponse> doIgnoreOp(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);
      boolean add = request.method().equals(POST);

      String cacheManagerName = request.variables().get("cache-manager");
      DefaultCacheManager cacheManager = invocationHelper.getServer().getCacheManager(cacheManagerName);

      if (cacheManager == null) return completedFuture(builder.status(NOT_FOUND).build());

      String cacheName = request.variables().get("cache");

      if (!cacheManager.getCacheNames().contains(cacheName)) {
         return completedFuture(builder.status(NOT_FOUND).build());
      }
      ServerManagement server = invocationHelper.getServer();
      ServerStateManager ignoreManager = server.getServerStateManager();
      return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<Void>>) () ->
                           add ? ignoreManager.ignoreCache(cacheName) : ignoreManager.unignoreCache(cacheName))
                     .thenApply(r -> builder.build());
   }

   private CompletionStage<RestResponse> listIgnored(RestRequest request) {
      String cacheManagerName = request.variables().get("cache-manager");
      DefaultCacheManager cacheManager = invocationHelper.getServer().getCacheManager(cacheManagerName);

      if (cacheManager == null) return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      Set<String> ignored = serverStateManager.getIgnoredCaches();
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(ignored), isPretty(request));
   }

   private CompletionStage<RestResponse> cacheManagers(RestRequest request) {
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(invocationHelper.getServer().cacheManagerNames()), isPretty(request));
   }

   private CompletionStage<RestResponse> connectorStartStop(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);
      String connectorName = request.variables().get("connector");
      ProtocolServer<?> connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());
      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      switch (request.getAction()) {
         case "start":
            return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
                  serverStateManager.connectorStart(connectorName).thenApply(r -> builder.build()));
         case "stop":
            if (connector.equals(invocationHelper.getProtocolServer()) || connector.equals(invocationHelper.getProtocolServer().getEnclosingProtocolServer())) {
               return completedFuture(builder.status(CONFLICT).entity(Messages.MSG.connectorMatchesRequest(connectorName)).build());
            } else {
               return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
                     serverStateManager.connectorStop(connectorName).thenApply(r -> builder.build()));
            }
      }
      throw Log.REST.unknownAction(request.getAction());
   }

   private CompletionStage<RestResponse> connectorStatus(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      String connectorName = request.variables().get("connector");

      ProtocolServer connector = getProtocolServer(request);
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
      return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            serverStateManager.connectorStatus(connectorName).thenApply(b -> builder.contentType(APPLICATION_JSON).entity(info.set("enabled", b)).build()));
   }

   private CompletionStage<RestResponse> connectorIpFilterList(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);

      ProtocolServer connector = getProtocolServer(request);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      return completedFuture(addEntityAsJson(ipFilterRulesAsJson(connector), builder, isPretty(request)).build());
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

   private CompletionStage<RestResponse> connectorIpFilterClear(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);

      String connectorName = request.variables().get("connector");
      ProtocolServer connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            serverStateManager.clearConnectorIpFilterRules(connectorName).thenApply(r -> builder.build()));
   }

   private CompletionStage<RestResponse> listConnectors(RestRequest request) {
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(invocationHelper.getServer().getProtocolServers().keySet()), isPretty(request));
   }

   private CompletionStage<RestResponse> connectorIpFilterSet(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);

      String connectorName = request.variables().get("connector");
      ProtocolServer<?> connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      Json json = Json.read(request.contents().asString());
      if (!json.isArray()) {
         throw Log.REST.invalidContent();
      }
      List<Json> list = json.asJsonList();
      List<IpSubnetFilterRule> rules = new ArrayList<>(list.size());
      for (Json o : list) {
         if (!o.has("type") || !o.has("cidr")) {
            throw Log.REST.missingArguments("type", "cidr");
         } else {
            rules.add(new IpSubnetFilterRule(o.at("cidr").asString(), IpFilterRuleType.valueOf(o.at("type").asString())));
         }
      }
      // Verify that none of the REJECT rules match the address from which the request was made
      if (connector.equals(invocationHelper.getProtocolServer()) || connector.equals(invocationHelper.getProtocolServer().getEnclosingProtocolServer())) {
         InetSocketAddress remoteAddress = request.getRemoteAddress();
         for (IpSubnetFilterRule rule : rules) {
            if (rule.ruleType() == IpFilterRuleType.REJECT && rule.matches(remoteAddress)) {
               return completedFuture(builder.status(CONFLICT).entity(Messages.MSG.rejectRuleMatchesRequestAddress(rule, remoteAddress)).build());
            }
         }
      }

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
            serverStateManager.setConnectorIpFilterRule(connectorName, rules).thenApply(r -> builder.build()));
   }

   private CompletionStage<RestResponse> memory(RestRequest request) {
      return asJsonResponseFuture(invocationHelper.newResponse(request), new JVMMemoryInfoInfo().toJson(), isPretty(request));
   }

   private CompletionStage<RestResponse> heapDump(RestRequest request) {
      boolean live = Boolean.parseBoolean(request.getParameter("live"));
      boolean pretty = isPretty(request);
      ServerManagement server = invocationHelper.getServer();
      return CompletableFuture.supplyAsync(() -> {
         try {
            Path dumpFile = Files.createTempFile(server.getServerDataPath(), "dump", ".hprof");
            Files.delete(dumpFile);
            new JVMMemoryInfoInfo().heapDump(dumpFile, live);
            return asJsonResponse(invocationHelper.newResponse(request), Json.object().set("filename", dumpFile.getFileName().toString()), pretty);
         } catch (IOException e) {
            throw Log.REST.heapDumpFailed(e);
         }
      }, blockingExecutor);
   }

   private CompletionStage<RestResponse> env(RestRequest request) {
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(System.getProperties()), isPretty(request));
   }

   private CompletionStage<RestResponse> info(RestRequest request) {
      return asJsonResponseFuture(invocationHelper.newResponse(request), SERVER_INFO.toJson(), isPretty(request));
   }

   private CompletionStage<RestResponse> threads(RestRequest request) {
      return completedFuture(invocationHelper.newResponse(request)
            .contentType(TEXT_PLAIN).entity(Util.threadDump())
            .build());
   }

   private CompletionStage<RestResponse> report(RestRequest request) {
      ServerManagement server = invocationHelper.getServer();
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      return Security.doAs(request.getSubject(), (PrivilegedAction<CompletionStage<RestResponse>>) () ->
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

   private CompletionStage<RestResponse> stop(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> {
         Security.doAs(request.getSubject(), (PrivilegedAction<?>) () -> {
            invocationHelper.getServer().serverStop(Collections.emptyList());
            return null;
         });

         return invocationHelper.newResponse(request)
               .status(NO_CONTENT).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> config(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML);
      responseBuilder.contentType(accept);
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(accept).prettyPrint(pretty).build()) {
         invocationHelper.getServer().serializeConfiguration(w);
      }
      responseBuilder.entity(sw.toString());
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> dataSourceList(RestRequest request) {
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(invocationHelper.getServer().getDataSources().keySet()), isPretty(request));
   }

   private CompletionStage<RestResponse> dataSourceTest(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);

      String name = request.variables().get("datasource");
      DataSource dataSource = invocationHelper.getServer().getDataSources().get(name);
      if (dataSource == null) return completedFuture(builder.status(NOT_FOUND).build());

      return CompletableFuture.supplyAsync(() -> {
         try (Connection connection = dataSource.getConnection()) {
            if (connection.isValid(0)) {
               builder.status(OK).entity(Messages.MSG.dataSourceTestOk(name));
            } else {
               builder.status(SERVICE_UNAVAILABLE).entity(Messages.MSG.dataSourceTestFail(name));
            }
         } catch (Exception e) {
            throw Util.unchecked(e);
         }
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   static class ServerInfo implements JsonSerialization {
      private static final Json json = Json.object("version", Version.printVersion());

      @Override
      public Json toJson() {
         return json;
      }
   }
}
