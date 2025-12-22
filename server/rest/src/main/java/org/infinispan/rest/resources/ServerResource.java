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
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.sql.DataSource;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.JVMMemoryInfoInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.Messages;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.server.core.transport.IpSubnetFilterRule;
import org.infinispan.server.core.transport.Transport;

import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * @since 10.0
 */
public class ServerResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private final Executor blockingExecutor;

   public ServerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.blockingExecutor = invocationHelper.getExecutor();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("server", "REST resource to manage the current server.")
            .invocation().methods(GET).path("/v2/server/")
               .handleWith(this::info)
            .invocation().methods(GET).path("/v2/server/config")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::config)
            .invocation().methods(GET).path("/v2/server/env")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::env)
            .invocation().methods(GET).path("/v2/server/memory")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::memory)
            .invocation().methods(POST).path("/v2/server/memory").withAction("heap-dump")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::heapDump)
            .invocation().methods(POST).path("/v2/server/").withAction("stop")
               .permission(AuthorizationPermission.ADMIN)
               .handleWith(this::stop)
            .invocation().methods(GET).path("/v2/server/overview-report")
               .permission(AuthorizationPermission.ADMIN)
               .handleWith(this::overviewReport)
            .invocation().methods(GET).path("/v2/server/threads")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::threads)
            .invocation().methods(GET).path("/v2/server/report")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER)
               .handleWith(this::report)
            .invocation().methods(GET).path("/v2/server/report/{nodeName}")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER)
               .handleWith(this::nodeReport)
            .invocation().methods(GET).path("/v2/server/ignored-caches/{cache-manager}")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::listIgnored)
            .invocation().methods(GET).path("/v2/server/ignored-caches")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::listIgnored)
            .invocation().methods(POST, DELETE).path("/v2/server/ignored-caches/{cache-manager}/{cache}")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::doIgnoreOp)
            .invocation().methods(POST, DELETE).path("/v2/server/ignored-caches/{cache}")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.SERVER)
               .handleWith(this::doIgnoreOp)
            .invocation().methods(GET).path("/v2/server/connections")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTION LIST")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::listConnections)
            .invocation().methods(GET).path("/v2/server/connectors")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR LIST")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::listConnectors)
            .invocation().methods(GET).path("/v2/server/connectors/{connector}")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR GET")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::connectorStatus)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}").withAction("start")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR START")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::connectorStartStop)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}").withAction("stop")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR STOP")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::connectorStartStop)
            .invocation().methods(GET).path("/v2/server/connectors/{connector}/ip-filter")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR FILTER GET")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::connectorIpFilterList)
            .invocation().methods(POST).path("/v2/server/connectors/{connector}/ip-filter")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR FILTER SET")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::connectorIpFilterSet)
            .invocation().methods(DELETE).path("/v2/server/connectors/{connector}/ip-filter")
               .permission(AuthorizationPermission.ADMIN).name("CONNECTOR FILTER DELETE")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::connectorIpFilterClear)
            .invocation().methods(GET).path("/v2/server/datasources")
               .permission(AuthorizationPermission.ADMIN).name("DATASOURCE LIST")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::dataSourceList)
            .invocation().methods(POST).path("/v2/server/datasources/{datasource}").withAction("test")
               .permission(AuthorizationPermission.ADMIN).name("DATASOURCE TEST")
               .auditContext(AuditContext.SERVER)
               .handleWith(this::dataSourceTest)
            .invocation().methods(GET).path("/v2/server/caches/defaults")
               .handleWith(this::getCacheConfigDefaultAttributes)
            .create();
   }

   private CompletionStage<RestResponse> doIgnoreOp(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);
      boolean add = request.method().equals(POST);

      RestCacheManager<Object> cacheManager = invocationHelper.getRestCacheManager();
      String cacheManagerName = request.variables().get("cache-manager");
      String cacheName = request.variables().get("cache");
      if (cacheName == null) {
         cacheName = cacheManagerName;
      }
      if (!cacheManager.getCacheNames().contains(cacheName)) {
         return completedFuture(builder.status(NOT_FOUND).build());
      }
      final String cacheNameFinal = cacheName;
      ServerManagement server = invocationHelper.getServer();
      ServerStateManager ignoreManager = server.getServerStateManager();
      return Security.doAs(request.getSubject(), () -> add ? ignoreManager.ignoreCache(cacheNameFinal) : ignoreManager.unignoreCache(cacheNameFinal))
            .thenApply(r -> builder.build());
   }

   private CompletionStage<RestResponse> listIgnored(RestRequest request) {
      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      Set<String> ignored = serverStateManager.getIgnoredCaches();
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(ignored), isPretty(request));
   }

   private CompletionStage<RestResponse> connectorStartStop(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);
      String connectorName = request.variables().get("connector");
      ProtocolServer<?> connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());
      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      switch (request.getAction()) {
         case "start":
            return Security.doAs(request.getSubject(), () ->
                  serverStateManager.connectorStart(connectorName).thenApply(r -> builder.build()));
         case "stop":
            if (connector.equals(invocationHelper.getProtocolServer()) || connector.equals(invocationHelper.getProtocolServer().getEnclosingProtocolServer())) {
               return completedFuture(builder.status(CONFLICT).entity(Messages.MSG.connectorMatchesRequest(connectorName)).build());
            } else {
               return Security.doAs(request.getSubject(), () ->
                     serverStateManager.connectorStop(connectorName).thenApply(r -> builder.build()));
            }
      }
      throw Log.REST.unknownAction(request.getAction());
   }

   private CompletionStage<RestResponse> connectorStatus(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      String connectorName = request.variables().get("connector");

      ProtocolServer<?> connector = getProtocolServer(request);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      Json info = Json.object()
            .set("name", connectorName)
            .set("ip-filter-rules", ipFilterRulesAsJson(connector))
            .set("default-cache", connector.getConfiguration().defaultCacheName());
      Transport transport = connector.getTransport();
      CompletableFuture<Integer> globalConnections;
      if (transport != null) {
         info.set("host", transport.getHostName())
               .set("port", transport.getPort())
               .set("local-connections", transport.getNumberOfLocalConnections())
               .set("io-threads", transport.getNumberIOThreads())
               .set("pending-tasks", transport.getPendingTasks())
               .set("total-bytes-read", transport.getTotalBytesRead())
               .set("total-bytes-written", transport.getTotalBytesWritten())
               .set("send-buffer-size", transport.getSendBufferSize())
               .set("receive-buffer-size", transport.getReceiveBufferSize());
         globalConnections = CompletableFuture.supplyAsync(transport::getNumberOfGlobalConnections, invocationHelper.getExecutor());
      } else {
         globalConnections = CompletableFutures.completedNull();
      }
      CompletableFuture<Boolean> connectorStatus = Security.doAs(request.getSubject(), () -> serverStateManager.connectorStatus(connectorName));
      return connectorStatus.thenCombine(globalConnections, (cs, gc) -> {
         info.set("enabled", cs);
         if (gc != null) {
            info.set("global-connections", gc);
         }
         return builder.contentType(APPLICATION_JSON).entity(info).build();
      });
   }

   private CompletionStage<RestResponse> connectorIpFilterList(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);

      ProtocolServer<?> connector = getProtocolServer(request);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      return completedFuture(addEntityAsJson(ipFilterRulesAsJson(connector), builder, isPretty(request)).build());
   }

   private Json ipFilterRulesAsJson(ProtocolServer<?> connector) {
      Collection<IpSubnetFilterRule> rules = connector.getConfiguration().ipFilter().rules();
      Json array = Json.array();
      for (IpSubnetFilterRule rule : rules) {
         array.add(Json.object().set("type", rule.ruleType().name().toLowerCase()).set("from", rule.cidr()));
      }
      return array;
   }

   private ProtocolServer<?> getProtocolServer(RestRequest restRequest) {
      String connectorName = restRequest.variables().get("connector");
      return invocationHelper.getServer().getProtocolServers().get(connectorName);
   }

   private CompletionStage<RestResponse> listConnections(RestRequest request) {
      boolean global = Boolean.parseBoolean(request.getParameter("global"));
      if (global) {
         List<Json> results = Collections.synchronizedList(new ArrayList<>());
         return SecurityActions.getClusterExecutor(invocationHelper.getProtocolServer().getCacheManager())
               .submitConsumer(
                     ecm -> {
                        GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(ecm);
                        BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
                        ServerStateManager ssm = bcr.getComponent(ServerStateManager.class).wired();
                        return CompletableFutures.uncheckedAwait(ssm.listConnections()).toString();
                     }, (ignore, s, t) -> {
                        if (t != null) {
                           throw CompletableFutures.asCompletionException(t);
                        } else {
                           results.add(Json.read(s));
                        }
                     })
               .thenApply(ignore -> {
                  Json all = Json.array();
                  for (Json result : results) {
                     for (Json c : result.asJsonList()) {
                        all.add(c);
                     }
                  }
                  return asJsonResponse(invocationHelper.newResponse(request), all, isPretty(request));
               });
      } else {
         ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
         return serverStateManager.listConnections().thenApply(j -> asJsonResponse(invocationHelper.newResponse(request), j, isPretty(request)));
      }
   }

   private CompletionStage<RestResponse> connectorIpFilterClear(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request).status(NO_CONTENT);

      String connectorName = request.variables().get("connector");
      ProtocolServer<?> connector = invocationHelper.getServer().getProtocolServers().get(connectorName);
      if (connector == null) return completedFuture(builder.status(NOT_FOUND).build());

      ServerStateManager serverStateManager = invocationHelper.getServer().getServerStateManager();
      return Security.doAs(request.getSubject(), () -> serverStateManager.clearConnectorIpFilterRules(connectorName).thenApply(r -> builder.build()));
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
      return Security.doAs(request.getSubject(), () -> serverStateManager.setConnectorIpFilterRule(connectorName, rules).thenApply(r -> builder.build()));
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
      return asJsonResponseFuture(invocationHelper.newResponse(request), new ServerInfo(invocationHelper.getServer()).toJson(), isPretty(request));
   }

   private CompletionStage<RestResponse> overviewReport(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> asJsonResponse(invocationHelper.newResponse(request),
            invocationHelper.getServer().overviewReport(),
            isPretty(request)), blockingExecutor);
   }

   private CompletionStage<RestResponse> threads(RestRequest request) {
      return completedFuture(invocationHelper.newResponse(request)
            .contentType(TEXT_PLAIN).entity(Util.threadDump())
            .build());
   }

   private CompletionStage<RestResponse> report(RestRequest request) {
      ServerManagement server = invocationHelper.getServer();
      return Security.doAs(request.getSubject(), () -> server.getServerReport().handle((path, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }

               return createReportResponse(request, path.toFile(), invocationHelper.getRestCacheManager().getNodeName());
            })
      );
   }

   private RestResponse createReportResponse(RestRequest request, Object report, String filename) {
      return invocationHelper.newResponse(request)
            .contentType(MediaType.APPLICATION_GZIP_TYPE)
            .header("Content-Disposition",
                  String.format("attachment; filename=\"%s-%s-%3$tY%3$tm%3$td%3$tH%3$tM%3$tS-report.tar.gz\"",
                        Version.getBrandName().toLowerCase().replaceAll("\\s", "-"),
                        filename,
                        Calendar.getInstance())
            )
            .entity(report)
            .build();
   }

   private CompletionStage<RestResponse> nodeReport(RestRequest request) {
      String targetName = request.variables().get("nodeName");
      EmbeddedCacheManager cacheManager = invocationHelper.getProtocolServer().getCacheManager();
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      List<Address> members = cacheManager.getMembers();

      if (invocationHelper.getRestCacheManager().getNodeName().equals(targetName)) {
         return report(request);
      }

      if (members == null) {
         return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND).build());
      }

      ByRef<byte[]> response = new ByRef<>(null);
      return SecurityActions.getClusterExecutor(cacheManager)
            .submitConsumer(ecm -> {
               CacheManagerInfo cmi = ecm.getCacheManagerInfo();
               if (!cmi.getNodeName().equals(targetName)) return null;

               GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(ecm);
               BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
               ServerStateManager ssm = bcr.getComponent(ServerStateManager.class).wired();

               Path reportPath = CompletableFutures.uncheckedAwait(ssm.managedServer().getServerReport().toCompletableFuture());
               try {
                  return Files.readAllBytes(reportPath);
               } catch (IOException e) {
                  throw new CacheException(String.format("Failed reading '%s' at node '%s'", reportPath, targetName), e);
               }
            }, (ignore, info, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }

               if (info != null) {
                  response.set(info);
               }
            })
            .thenApply(ignore -> {
               byte[] report = response.get();
               if (report == null) {
                  return responseBuilder.status(NOT_FOUND).build();
               }

               return createReportResponse(request, report, targetName);
            });
   }

   private CompletionStage<RestResponse> stop(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> {
         Security.doAs(request.getSubject(), () -> invocationHelper.getServer().serverStop(Collections.emptyList()));
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

   private CompletionStage<RestResponse> getCacheConfigDefaultAttributes(RestRequest request) {
      Configuration configuration = new ConfigurationBuilder().build();
      Map<String, Object> attributes = new LinkedHashMap<>();
      allAttributes(configuration, attributes, configuration.elementName());
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(attributes), isPretty(request));
   }

   private static void allAttributes(ConfigurationElement<?> element, Map<String, Object> attributes, String prefix) {
      Map<String, Object> attributeMap = new LinkedHashMap<>();
      for (Attribute<?> attribute : element.attributes().attributes()) {
         AttributeDefinition<?> definition = attribute.getAttributeDefinition();
         String value;
         if (attribute.getInitialValue() == null) {
            value = null;
         } else {
            value = attribute.getInitialValue().toString();
         }
         attributeMap.put(definition.name(), value);
      }

      Map<String, Object> relative = attributes;
      String[] path = prefix.split("\\.");
      for (int i = 0; i < path.length - 1; i++) {
         String key = path[i];
         relative = (Map<String, Object>) relative.computeIfAbsent(key, ignore -> new LinkedHashMap<>());
      }
      relative.put(path[path.length - 1], attributeMap);

      for (ConfigurationElement<?> child : element.children()) {
         allAttributes(child, attributes, prefix + "." + child.elementName());
      }
   }

   static class ServerInfo implements JsonSerialization {
      private final Json json;

      public ServerInfo(ServerManagement server) {
         json = Json.object("version", Version.printVersion(), "cache-manager-name", server.getCacheManager().getName());
      }

      @Override
      public Json toJson() {
         return json;
      }
   }
}
