package org.infinispan.rest.resources;

import static java.util.Map.entry;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.CacheOperationsHelper;
import org.infinispan.rest.resources.mcp.McpConstants;
import org.infinispan.rest.resources.mcp.McpInputSchema;
import org.infinispan.rest.resources.mcp.McpProperty;
import org.infinispan.rest.resources.mcp.McpTool;
import org.infinispan.rest.resources.mcp.McpType;
import org.infinispan.security.actions.SecurityActions;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 16.0
 */
public class McpResource implements ResourceHandler {
   public static final String CACHE_NAME = "cacheName";
   public static final String COUNTER_NAME = "counterName";
   private final InvocationHelper invocationHelper;
   private final Map<String, McpTool> TOOLS;

   public McpResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.TOOLS = Map.ofEntries(
            entry(
                  "getCacheNames",
                  new McpTool(
                        "getCacheNames",
                        "Retrieves all the available cache names",
                        new McpInputSchema(McpType.OBJECT),
                        this::getCacheNames
                  )
            ),
            entry(
                  "createCache",
                  new McpTool(
                        "createCache",
                        "Creates a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              )
                        ),
                        this::createCache
                  )
            ),
            entry(
                  "getCacheEntry",
                  new McpTool(
                        "getCacheEntry",
                        "Retrieves an value from a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "key",
                                    McpType.STRING,
                                    "The key of the entry",
                                    true
                              )
                        ),
                        this::getCacheValue
                  )
            ),
            entry(
                  "setCacheEntry",
                  new McpTool(
                        "setCacheEntry",
                        "Inserts/updates an entry in a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "key",
                                    McpType.STRING,
                                    "The key of the entry",
                                    true
                              ),
                              new McpProperty(
                                    "value",
                                    McpType.STRING,
                                    "The value of the entry",
                                    true
                              ),
                              new McpProperty(
                                    "lifespan",
                                    McpType.NUMBER,
                                    "The lifespan of the entry in milliseconds",
                                    false
                              ),
                              new McpProperty(
                                    "maxIdle",
                                    McpType.NUMBER,
                                    "The maximum idle time of the entry in milliseconds",
                                    false
                              )
                        ),
                        this::setCacheValue
                  )
            ),
            entry(
                  "deleteCacheEntry",
                  new McpTool(
                        "deleteCacheEntry",
                        "Deletes an entry from a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "key",
                                    McpType.STRING,
                                    "The key of the entry",
                                    true
                              )
                        ),
                        this::deleteCacheValue
                  )
            ),
            entry(
                  "queryCache",
                  new McpTool(
                        "queryCache",
                        "Queries a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "query",
                                    McpType.STRING,
                                    "The Ickle query",
                                    true
                              )
                        ),
                        this::queryCache
                  )
            ),
            entry(
                  "getSchemas",
                  new McpTool(
                        "getSchemas",
                        "Retrieves all the available schemas",
                        new McpInputSchema(McpType.OBJECT),
                        this::getSchemas
                  )
            ),
            entry(
                  "getCounterNames",
                  new McpTool(
                        "getCounterNames",
                        "Retrieves all the available counter names",
                        new McpInputSchema(McpType.OBJECT),
                        this::getCounterNames
                  )
            ),
            entry(
                  "getCounter",
                  new McpTool(
                        "getCounter",
                        "Retrieves the value of a counter",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              )
                        ),
                        this::getCounter
                  )
            ),
            entry(
                  "increment",
                  new McpTool(
                        "increment",
                        "Increments the value of a counter",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              )
                        ),
                        this::incrementCounter
                  )
            ),
            entry(
                  "decrement",
                  new McpTool(
                        "decrement",
                        "Increments the value of a counter",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              )
                        ),
                        this::decrementCounter
                  )
            )
      );
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("mcp", "Model Context Protocol")
            .invocation().methods(GET, POST).path("/v3/mcp")
            .handleWith(this::mcp)
            .create();
   }

   private CompletionStage<Json> getSchemas(RestRequest request, Json json) {
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);
      return CompletableFuture.supplyAsync(() -> {
         Json schemas = Json.array();
         for (Map.Entry<Object, Object> entry : cache.entrySet()) {
            schemas.add(Json.object().set("name", entry.getKey()).set("schema", entry.getValue()));
         }
         return schemas;
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> mcp(RestRequest request) {
      String content = request.contents().asString();
      MediaType contentType = request.contentType();
      if (MediaType.APPLICATION_JSON.equals(contentType)) {
         // Single call
         Json json = Json.read(content);
         if (!json.isObject()) {
            throw Log.REST.invalidContent();
         }
         if (!"2.0".equals(json.at(McpConstants.JSONRPC).asString())) {
            throw Log.REST.invalidContent();
         }
         String method = json.at("method").asString();
         return switch (method) {
            // These should be the most frequently invoked methods
            case "resources/read" -> mcpResourcesRead(request, json);
            case "tools/call" -> mcpToolsCall(request, json);
            // All other calls
            case "completion/complete" -> mcpCompletionComplete(request, json);
            case "initialize" -> mcpInitialize(request, json);
            // case "logging/setLevel" -> mcpLoggingSetLevel(request, json);
            case "prompts/get" -> mcpPromptsGet(request, json);
            case "prompts/list" -> mcpPromptsList(request, json);
            case "resources/list" -> mcpResourcesList(request, json);
            case "resources/subscribe" -> mcpResourcesSubscribe(request, json);
            case "resources/templates/list" -> mcpResourcesTemplatesList(request, json);
            case "resources/unsubscribe" -> mcpResourcesUnubscribe(request, json);
            case "notifications/initialized" -> mcpNotificationsInitialized(request, json);
            case "tools/list" -> mcpToolsList(request, json);
            default -> throw Log.REST.invalidContent();
         };
      } else {
         // SSE
         NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
         responseBuilder.contentType(TEXT_EVENT_STREAM).entity(newEventStream());
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
   }

   private EventStream newEventStream() {
      /*
       * Messages that can be returned here
       * "notifications/message"
       * "notifications/prompts/list_changed"
       * "notifications/resources/list_changed"
       * "notifications/resources/updated"
       * "notifications/tools/list_changed"
       */
      return new EventStream(null, () -> {
      });
   }

   private CompletionStage<RestResponse> mcpPromptsGet(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      String name = params.at("name").asString();
      // TODO: get any prompt arguments
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("prompts", Json.array())
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpPromptsList(RestRequest request, Json json) {
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("prompts", Json.array())
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpCompletionComplete(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      Json argument = params.at("argument");
      String argumentName = argument.at("name").asString();
      String argumentValue = argument.at("value").asString();
      Json ref = params.at("ref");
      String refType = ref.at("type").asString();
      String refURI = ref.at("uri").asString();
      //TODO: complete prompt (ref/prompt) or resource template (ref/resource) arguments. For now we return an error
      Json response = rpcResponse(json)
            .set("error", Json.object().set("code", McpConstants.METHOD_NOT_FOUND).set("message", "Resource template completion does not exist"));
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpInitialize(RestRequest request, Json json) {
      Json params = json.at("params");
      String protocolVersion = params.has("protocolVersion") ? params.at("protocolVersion").asString() : McpConstants.MCP_VERSION;
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("capabilities", Json.object()
                              .set("resources", Json.object()
                                    .set("subscribe", false) // whether the client can subscribe to be notified of changes to individual resources.
                                    .set("listChanged", false) // whether the server will emit notifications when the list of available resources changes.
                              )
                              //.set("completions", Json.object()) // Enable when we support completions
                              // .set("logging", Json.object()) // Enable when we support logging
                              .set("tools", Json.object()
                                    .set("listChanged", false) //indicates whether the server will emit notifications when the list of available tools changes.
                              )
                        //.set("prompts", Json.object()) // Enable when we support prompts
                  )
                  .set("serverInfo", Json.object()
                        .set("version", Version.getVersion())
                        .set("name", Version.getBrandName())
                  )
                  .set("protocolVersion", protocolVersion)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request).header(McpConstants.MCP_SESSION_ID, UUID.randomUUID().toString()), response, false);
   }

   private static Json rpcResponse(Json request) {
      Json json = Json.object()
            .set(McpConstants.JSONRPC, "2.0");
      if (request.has("id")) {
         json.set("id", request.at("id").asInteger());
      }
      return json;
   }

   private CompletionStage<RestResponse> mcpResourcesList(RestRequest request, Json json) {
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("resources", Json.array())
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpResourcesTemplatesList(RestRequest request, Json json) {
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("resourceTemplates", Json.array()
                        .add(Json.object()
                                    .set("uriTemplate", "infinispan+cache://{cacheName}/{key}")
                                    .set(McpConstants.NAME, "cache value")
                                    .set(McpConstants.DESCRIPTION, "Retrieves a value from a cache")
                              //.set("mimeType", "text/plain")
                        ).add(Json.object()
                              .set("uriTemplate", "infinispan+counter://{countername}")
                              .set(McpConstants.NAME, "counter value")
                              .set(McpConstants.DESCRIPTION, "Retrieves a value from a counter")
                        )
                  )
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpResourcesRead(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/read not supported");
   }

   private CompletionStage<RestResponse> mcpResourcesUnubscribe(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/unsubscribe not supported");
   }

   private CompletionStage<RestResponse> mcpResourcesSubscribe(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/subscribe not supported");
   }

   private CompletionStage<RestResponse> mcpNotificationsInitialized(RestRequest request, Json json) {
      return unimplemented(request, json, "notifications/initialized supported");
   }

   private CompletionStage<RestResponse> unimplemented(RestRequest request, Json json, String message) {
      Json response = rpcResponse(json)
            .set("error", Json.object().set("code", McpConstants.METHOD_NOT_FOUND).set("message", message));
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpToolsList(RestRequest request, Json json) {
      Json tools = Json.array();
      for (McpTool tool : TOOLS.values()) {
         tools.add(tool.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("tools", tools)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpToolsCall(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      String name = params.at(McpConstants.NAME).asString();
      McpTool mcpTool = TOOLS.get(name);
      if (mcpTool == null) {
         Json response = rpcResponse(json)
               .set("error", Json.object().set("code", McpConstants.METHOD_NOT_FOUND).set("message", "Tool not found: " + name));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }
      Json arguments = params.at("arguments");
      Json response = rpcResponse(json);
      return mcpTool.callback().apply(request, arguments).handle((j, t) -> {
         response.set(McpConstants.RESULT, Json.object()
               .set("isError", t != null)
               .set("content", j)
         );
         NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
         return builder.entity(response.toString()).contentType(MediaType.APPLICATION_JSON).status(HttpResponseStatus.OK).build();
      });
   }

   private CompletionStage<Json> getCacheNames(RestRequest request, Json args) {
      Collection<String> cacheNames = invocationHelper.getRestCacheManager().getAccessibleCacheNames();
      return CompletableFuture.completedFuture(Json.array(textResult(Json.make(cacheNames).toString())));
   }

   private CompletionStage<Json> createCache(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      EmbeddedCacheManagerAdmin admin = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      return CompletableFuture.supplyAsync(() -> {
         admin.createCache(cacheName, builder.build());
         return Json.array();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      MediaType requestedMediaType = negotiateMediaType(cache, invocationHelper.getEncoderRegistry(), request);
      return invocationHelper.getRestCacheManager()
            .getInternalEntry(cacheName, key, MediaType.TEXT_PLAIN, requestedMediaType, request)
            .thenApply(entry -> Json.array(textResult(entry.getValue().toString())));
   }

   private static Json textResult(String value) {
      return Json.object().set("text", value).set("type", "text");
   }

   private CompletionStage<Json> setCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      String value = args.at("value").asString();
      Long lifespan = args.has("lifespan") ? args.at("lifespan").asLong() : null;
      Long maxidle = args.has("maxidle") ? args.at("maxidle").asLong() : null;
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, MediaType.TEXT_PLAIN, MediaType.TEXT_PLAIN, request);
      Configuration config = SecurityActions.getCacheConfiguration(cache);
      final Metadata metadata = CacheOperationsHelper.createMetadata(config, lifespan, maxidle);
      return cache.putAsync(key, value, metadata).thenApply(__ -> Json.array());
   }

   private CompletionStage<Json> deleteCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      return invocationHelper.getRestCacheManager()
            .remove(cacheName, key, MediaType.TEXT_PLAIN, request)
            .thenApply(entry -> Json.array());
   }

   private CompletionStage<Json> queryCache(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String query = args.at("query").asString();
      return invocationHelper.getRestCacheManager()
            .remove(cacheName, query, MediaType.TEXT_PLAIN, request)
            .thenApply(entry -> Json.array());
   }

   private CompletionStage<Json> getCounterNames(RestRequest request, Json args) {
      Collection<String> counterNames = invocationHelper.getCounterManager().getCounterNames();
      return CompletableFuture.completedFuture(Json.array(textResult(Json.make(counterNames).toString())));
   }

   private CompletionStage<Json> getCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getWeakCounterAsync(counterName)
            .thenApply(WeakCounter::getValue)
            .thenApply(v -> Json.array(textResult(v.toString())));
   }

   private CompletionStage<Json> incrementCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getWeakCounterAsync(counterName)
            .thenApply(WeakCounter::increment)
            .thenApply(__ -> Json.array());
   }

   private CompletionStage<Json> decrementCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getWeakCounterAsync(counterName)
            .thenApply(WeakCounter::decrement)
            .thenApply(__ -> Json.array());
   }
}
