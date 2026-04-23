package org.infinispan.rest.resources;

import static java.util.Map.entry;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.CacheOperationsHelper;
import org.infinispan.rest.resources.mcp.McpArgument;
import org.infinispan.rest.resources.mcp.McpConstants;
import org.infinispan.rest.resources.mcp.McpInputSchema;
import org.infinispan.rest.resources.mcp.McpPrompt;
import org.infinispan.rest.resources.mcp.McpPromptMessage;
import org.infinispan.rest.resources.mcp.McpProperty;
import org.infinispan.rest.resources.mcp.McpResource;
import org.infinispan.rest.resources.mcp.McpResourceTemplate;
import org.infinispan.rest.resources.mcp.McpTool;
import org.infinispan.rest.resources.mcp.McpType;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.server.core.query.impl.RemoteQueryManager;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 16.0
 */
public class McpServerResource implements ResourceHandler {
   public static final String CACHE_NAME = "cacheName";
   public static final String COUNTER_NAME = "counterName";
   private final InvocationHelper invocationHelper;
   private final Map<String, McpTool> TOOLS;
   private static final List<McpResource> RESOURCES = List.of(
         new McpResource(
               "infinispan+logs://server?lines=200",
               "server log",
               """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/HEALTH.
               Returns info about server status, health, errors, warnings,
               exceptions, startup/shutdown events, and troubleshooting
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://audit?lines=200",
               "audit log",
               """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/SECURITY.
               Returns infor about security events: authentication attempts,
               authorization failures, suspicious activity
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://rest-access?lines=200",
               "REST access log",
                """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/WORKLOAD.
               Returns info about REST API usage patterns: request rates,
               errors (4xx/5xx), slow endpoints, client IPs. Useful also
               for security auditing, i.e. detecting DoS attacks or suspicious
               activity
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://gc?lines=200",
               "gc log",
               """
               PRIMARY SOURCE OF INFORMATION FOR VM STATUS/MEMORY.
               Returns info about JVM garbage collection events, heap usage,
               memory allocation, GC pauses, and overall memory pressure on
               the running JVM instance
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://hotrod-access?lines=200",
               "Hot Rod access log",
               """
               PRIMARY SOURCE OF INFORMATION FOR HOTROD PROTOCOL WORKLOAD.
               Returns info about Hot Rod client connections and requests:
               request rates, response times, errors, connection patterns,
               and client IPs
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://memcached-access?lines=200",
               "Memcached access log",
               """
               PRIMARY SOURCE OF INFORMATION FOR MEMCACHED PROTOCOL WORKLOAD.
               Returns info about Memcached client connections and requests:
               request rates, response times, errors, connection patterns,
               and client IPs
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://resp-access?lines=200",
               "RESP access log",
               """
               PRIMARY SOURCE OF INFORMATION FOR REDIS PROTOCOL WORKLOAD.
               Returns info about RESP (Redis) client connections and requests:
               request rates, response times, errors, connection patterns,
               and client IPs
               """,
               "text/plain"
         )
   );
   private static final List<McpResourceTemplate> RESOURCE_TEMPLATES = List.of(
         new McpResourceTemplate(
               "infinispan+cache://{cacheName}/{key}",
               "cache value",
               "Runtime data information: retrieves a value from a cache",
               null
         ),
         new McpResourceTemplate(
               "infinispan+counter://{countername}",
               "counter value",
               "Runtime data information: retrieves a value from a counter",
               null
         ),
         new McpResourceTemplate(
               "infinispan+logs://{logType}?lines={lines}",
               "server logs",
               """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/HEALTH. Useful to retrieve different types of server logs. Primary source
               of information to monitor server status, server health, troubleshoot
               issues, and audit security-related events.""",
               "text/plain"
         )
   );

   private static final List<McpPrompt> PROMPTS = List.of(
         new McpPrompt(
               "find-documentation",
               """
                  Helps find relevant Infinispan documentation on the official website for a specific topic. Primary source
                  of information MUST BE https://infinispan.org/documentation
               """,
               "Find Infinispan Documentation",
               new McpArgument("topic", "What to search for in the documentation (e.g., 'cache configuration', 'Hot Rod client')", true),
               new McpArgument("context", "Additional context about your goal or use case (optional)", false)
         ),
         new McpPrompt(
               "find-documentation-guided",
               "Helps find relevant Infinispan documentation with topic suggestions for common areas",
               "Find Infinispan Documentation (Guided)",
               new McpArgument("topic", "What to search for in the documentation (e.g., 'cache configuration', 'Hot Rod client')", true),
               new McpArgument("context", "Additional context about your goal or use case (optional)", false)
         )
   );

   public McpServerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.TOOLS = Map.ofEntries(
            entry(
                  "getCacheNames",
                  new McpTool(
                        "getCacheNames",
                        "Runtime data inventory: retrieves all the available cache names. For server status/health, use log resources instead.",
                        new McpInputSchema(McpType.OBJECT),
                        this::getCacheNames
                  )
            ),
            entry(
                  "createCache",
                  new McpTool(
                        "createCache",
                        "Runtime data modification: creates a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "configuration",
                                    McpType.STRING,
                                    "The cache configuration in JSON format. If not provided, a default distributed synchronous cache with protostream encoding will be created.",
                                    false
                              )
                        ),
                        this::createCache
                  )
            ),
            entry(
                  "deleteCache",
                  new McpTool(
                        "deleteCache",
                        "Runtime data modification: deletes a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache to delete",
                                    true
                              )
                        ),
                        this::deleteCache
                  )
            ),
            entry(
                  "getCacheEntry",
                  new McpTool(
                        "getCacheEntry",
                        "Runtime data retrieval: retrieves a value from a cache",
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
                                    "mediaType",
                                    McpType.STRING,
                                    "The media type for the value (e.g. text/plain, application/json). Defaults to text/plain",
                                    false
                              )
                        ),
                        this::getCacheValue
                  )
            ),
            entry(
                  "setCacheEntry",
                  new McpTool(
                        "setCacheEntry",
                        "Runtime data modification: inserts/updates an entry in a cache",
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
                              ),
                              new McpProperty(
                                    "mediaType",
                                    McpType.STRING,
                                    "The media type for the value (e.g. text/plain, application/json). Defaults to text/plain",
                                    false
                              )
                        ),
                        this::setCacheValue
                  )
            ),
            entry(
                  "setCacheEntries",
                  new McpTool(
                        "setCacheEntries",
                        "Runtime data modification: inserts/updates multiple entries in a cache in a single operation",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "entries",
                                    McpType.STRING,
                                    "A JSON array of entry objects, each with 'key' (string), 'value' (string), and optionally 'mediaType' (string, defaults to text/plain). Example: [{\"key\":\"k1\",\"value\":\"v1\"},{\"key\":\"k2\",\"value\":\"{}\",\"mediaType\":\"application/json\"}]",
                                    true
                              ),
                              new McpProperty(
                                    "lifespan",
                                    McpType.NUMBER,
                                    "The lifespan of all entries in milliseconds",
                                    false
                              ),
                              new McpProperty(
                                    "maxIdle",
                                    McpType.NUMBER,
                                    "The maximum idle time of all entries in milliseconds",
                                    false
                              )
                        ),
                        this::setCacheValues
                  )
            ),
            entry(
                  "deleteCacheEntry",
                  new McpTool(
                        "deleteCacheEntry",
                        "Runtime data modification: deletes an entry from a cache",
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
                        "Runtime data retrieval: queries a cache using Ickle query language",
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
                        "Runtime data inventory: retrieves all the available schemas. For server status/health, use log resources instead.",
                        new McpInputSchema(McpType.OBJECT),
                        this::getSchemas
                  )
            ),
            entry(
                  "registerSchema",
                  new McpTool(
                        "registerSchema",
                        "Runtime data modification: registers or updates a Protobuf schema",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    "schemaName",
                                    McpType.STRING,
                                    "The schema name (e.g. person.proto)",
                                    true
                              ),
                              new McpProperty(
                                    "schema",
                                    McpType.STRING,
                                    "The Protobuf schema content",
                                    true
                              )
                        ),
                        this::registerSchema
                  )
            ),
            entry(
                  "getCacheConfiguration",
                  new McpTool(
                        "getCacheConfiguration",
                        "Runtime data retrieval: retrieves the configuration of a cache in JSON format",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              )
                        ),
                        this::getCacheConfiguration
                  )
            ),
            entry(
                  "getCacheStats",
                  new McpTool(
                        "getCacheStats",
                        "Runtime data retrieval: retrieves statistics for a cache (hits, misses, evictions, stores, timings, memory usage)",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              )
                        ),
                        this::getCacheStats
                  )
            ),
            entry(
                  "getClusterInfo",
                  new McpTool(
                        "getClusterInfo",
                        "Runtime data retrieval: retrieves cluster information including members, coordinator, status, rebalancing state, and sites",
                        new McpInputSchema(McpType.OBJECT),
                        this::getClusterInfo
                  )
            ),
            entry(
                  "getServerConfiguration",
                  new McpTool(
                        "getServerConfiguration",
                        "Runtime data retrieval: retrieves the full server configuration including endpoints, security, threading, JGroups, and cache container settings",
                        new McpInputSchema(McpType.OBJECT),
                        this::getServerConfiguration
                  )
            ),
            entry(
                  "createCounter",
                  new McpTool(
                        "createCounter",
                        "Runtime data modification: creates a counter. Types: WEAK (fast, eventual consistency), UNBOUNDED_STRONG (precise, no bounds), BOUNDED_STRONG (precise, with upper/lower bounds)",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              ),
                              new McpProperty(
                                    "type",
                                    McpType.STRING,
                                    "The counter type: WEAK, UNBOUNDED_STRONG, or BOUNDED_STRONG",
                                    true
                              ),
                              new McpProperty(
                                    "initialValue",
                                    McpType.NUMBER,
                                    "The initial value (default 0)",
                                    false
                              ),
                              new McpProperty(
                                    "storage",
                                    McpType.STRING,
                                    "The storage mode: VOLATILE (default, lost on restart) or PERSISTENT (survives restart)",
                                    false
                              ),
                              new McpProperty(
                                    "upperBound",
                                    McpType.NUMBER,
                                    "The upper bound (inclusive). Only for BOUNDED_STRONG counters",
                                    false
                              ),
                              new McpProperty(
                                    "lowerBound",
                                    McpType.NUMBER,
                                    "The lower bound (inclusive). Only for BOUNDED_STRONG counters",
                                    false
                              )
                        ),
                        this::createCounter
                  )
            ),
            entry(
                  "getCounterNames",
                  new McpTool(
                        "getCounterNames",
                        "Runtime data inventory: retrieves all the available counter names. For server status/health, use log resources instead.",
                        new McpInputSchema(McpType.OBJECT),
                        this::getCounterNames
                  )
            ),
            entry(
                  "getCounter",
                  new McpTool(
                        "getCounter",
                        "Runtime data retrieval: retrieves the value of a counter",
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
                        "Runtime data modification: increments the value of a counter",
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
                        "Runtime data modification: decrements the value of a counter",
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
            ),
            entry(
                  "getJvmMemory",
                  new McpTool(
                        "getJvmMemory",
                        "JVM diagnostics: retrieves heap/non-heap memory usage, garbage collector stats, and memory pool details",
                        new McpInputSchema(McpType.OBJECT),
                        this::getJvmMemory
                  )
            ),
            entry(
                  "getJvmThreads",
                  new McpTool(
                        "getJvmThreads",
                        "JVM diagnostics: retrieves thread counts, peak threads, deadlock detection, and optionally a full thread dump",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    "threadDump",
                                    McpType.BOOLEAN,
                                    "Whether to include a full thread dump (default false, can be large)",
                                    false
                              )
                        ),
                        this::getJvmThreads
                  )
            ),
            entry(
                  "getJvmInfo",
                  new McpTool(
                        "getJvmInfo",
                        "JVM diagnostics: retrieves JVM version, uptime, input arguments, available processors, and OS info",
                        new McpInputSchema(McpType.OBJECT),
                        this::getJvmInfo
                  )
            )
      );
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("mcp", "Model Context Protocol")
            .invocation().methods(GET, POST).path("/v3/mcp")
            .request("JSON-RPC 2.0 request", false, java.util.Map.of(MediaType.APPLICATION_JSON, org.infinispan.rest.framework.openapi.Schema.NONE))
            .handleWith(this::mcp)
            .create();
   }

   private CompletionStage<Json> getSchemas(RestRequest request, Json json) {
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);
      return CompletableFuture.supplyAsync(() -> {
         Json schemas = Json.array();
         for (Map.Entry<Object, Object> entry : cache.entrySet()) {
            String name = entry.getKey().toString();
            if (!name.endsWith(".errors")) {
               schemas.add(Json.object().set("name", name).set("schema", entry.getValue()));
            }
         }
         return Json.array(textResult(schemas.toString()));
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> registerSchema(RestRequest request, Json args) {
      String schemaName = args.at("schemaName").asString();
      if (!schemaName.endsWith(".proto")) {
         schemaName = schemaName + ".proto";
      }
      String schema = args.at("schema").asString();
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager()
            .getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);
      String finalSchemaName = schemaName;
      return cache.putAsync(schemaName, schema)
            .thenCompose(__ -> cache.getAsync(finalSchemaName + ".errors"))
            .thenApply(errors -> {
               if (errors != null && !((String) errors).isEmpty()) {
                  return Json.array(textResult("Schema '" + finalSchemaName + "' registered with errors: " + errors));
               }
               return Json.array(textResult("Schema '" + finalSchemaName + "' registered successfully"));
            });
   }

   private CompletionStage<Json> getCacheConfiguration(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      if (!cacheManager.getCacheConfigurationNames().contains(cacheName)) {
         return CompletableFuture.completedFuture(Json.array(textResult("Cache not found: " + cacheName)));
      }
      return CompletableFuture.supplyAsync(() -> {
         EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());
         Configuration config = SecurityActions.getCacheConfiguration(subjectCacheManager, cacheName);
         ByteArrayOutputStream entity = new ByteArrayOutputStream();
         try (ConfigurationWriter writer = ConfigurationWriter.to(entity).withType(MediaType.APPLICATION_JSON).prettyPrint(true).build()) {
            invocationHelper.getParserRegistry().serialize(writer, cacheName, config);
         } catch (Exception e) {
            return Json.array(textResult("Error serializing configuration: " + e.getMessage()));
         }
         return Json.array(textResult(entity.toString(StandardCharsets.UTF_8)));
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getCacheStats(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      return CompletableFuture.supplyAsync(() -> {
         Json stats = cache.getAdvancedCache().getStats().toJson();
         return Json.array(textResult(stats.toString()));
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getClusterInfo(RestRequest request, Json args) {
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      Json info = cacheManager.getCacheManagerInfo().toJson();
      return CompletableFuture.completedFuture(Json.array(textResult(info.toString())));
   }

   private CompletionStage<Json> getServerConfiguration(RestRequest request, Json args) {
      return CompletableFuture.supplyAsync(() -> {
         EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
         org.infinispan.configuration.global.GlobalConfiguration globalConfiguration =
               SecurityActions.getCacheManagerConfiguration(cacheManager);
         try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ConfigurationWriter writer = ConfigurationWriter.to(baos).withType(MediaType.APPLICATION_JSON).prettyPrint(true).build()) {
               invocationHelper.getParserRegistry().serialize(writer, globalConfiguration, java.util.Collections.emptyMap());
            }
            return Json.array(textResult(baos.toString(StandardCharsets.UTF_8)));
         } catch (Exception e) {
            return Json.array(textResult("Error serializing server configuration: " + e.getMessage()));
         }
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getJvmMemory(RestRequest request, Json args) {
      return CompletableFuture.supplyAsync(() -> {
         Json result = Json.object();
         java.lang.management.MemoryMXBean memoryBean = java.lang.management.ManagementFactory.getMemoryMXBean();
         java.lang.management.MemoryUsage heap = memoryBean.getHeapMemoryUsage();
         result.set("heap", Json.object()
               .set("used_mb", heap.getUsed() / (1024 * 1024))
               .set("committed_mb", heap.getCommitted() / (1024 * 1024))
               .set("max_mb", heap.getMax() / (1024 * 1024))
               .set("used_percent", heap.getMax() > 0 ? (heap.getUsed() * 100) / heap.getMax() : -1));
         java.lang.management.MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();
         result.set("non_heap", Json.object()
               .set("used_mb", nonHeap.getUsed() / (1024 * 1024))
               .set("committed_mb", nonHeap.getCommitted() / (1024 * 1024)));
         Json gcArray = Json.array();
         for (java.lang.management.GarbageCollectorMXBean gc : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
            gcArray.add(Json.object()
                  .set("name", gc.getName())
                  .set("collection_count", gc.getCollectionCount())
                  .set("collection_time_ms", gc.getCollectionTime()));
         }
         result.set("garbage_collectors", gcArray);
         Json pools = Json.array();
         for (java.lang.management.MemoryPoolMXBean pool : java.lang.management.ManagementFactory.getMemoryPoolMXBeans()) {
            java.lang.management.MemoryUsage usage = pool.getUsage();
            if (usage != null) {
               pools.add(Json.object()
                     .set("name", pool.getName())
                     .set("type", pool.getType().name())
                     .set("used_mb", usage.getUsed() / (1024 * 1024))
                     .set("max_mb", usage.getMax() / (1024 * 1024)));
            }
         }
         result.set("memory_pools", pools);
         return Json.array(textResult(result.toString()));
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getJvmThreads(RestRequest request, Json args) {
      return CompletableFuture.supplyAsync(() -> {
         java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory.getThreadMXBean();
         Json result = Json.object()
               .set("thread_count", threadBean.getThreadCount())
               .set("peak_thread_count", threadBean.getPeakThreadCount())
               .set("daemon_thread_count", threadBean.getDaemonThreadCount())
               .set("total_started_thread_count", threadBean.getTotalStartedThreadCount());
         long[] deadlocked = threadBean.findDeadlockedThreads();
         result.set("deadlocked_threads", deadlocked != null ? deadlocked.length : 0);
         if (deadlocked != null) {
            Json deadlockInfo = Json.array();
            for (java.lang.management.ThreadInfo ti : threadBean.getThreadInfo(deadlocked, true, true)) {
               if (ti != null) {
                  deadlockInfo.add(Json.object()
                        .set("name", ti.getThreadName())
                        .set("state", ti.getThreadState().name())
                        .set("lock_name", ti.getLockName())
                        .set("lock_owner", ti.getLockOwnerName()));
               }
            }
            result.set("deadlock_details", deadlockInfo);
         }
         boolean includeDump = args.has("threadDump") && args.at("threadDump").asBoolean();
         if (includeDump) {
            StringBuilder dump = new StringBuilder();
            for (java.lang.management.ThreadInfo ti : threadBean.dumpAllThreads(true, true)) {
               dump.append(ti.toString());
            }
            result.set("thread_dump", dump.toString());
         }
         return Json.array(textResult(result.toString()));
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getJvmInfo(RestRequest request, Json args) {
      return CompletableFuture.supplyAsync(() -> {
         Json result = Json.object();
         java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
         result.set("jvm_name", runtime.getVmName());
         result.set("jvm_vendor", runtime.getVmVendor());
         result.set("jvm_version", runtime.getVmVersion());
         result.set("spec_version", runtime.getSpecVersion());
         result.set("uptime_seconds", runtime.getUptime() / 1000);
         result.set("start_time", runtime.getStartTime());
         result.set("input_arguments", Json.make(runtime.getInputArguments()));
         result.set("classpath", runtime.getClassPath());
         java.lang.management.OperatingSystemMXBean os = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
         result.set("os", Json.object()
               .set("name", os.getName())
               .set("version", os.getVersion())
               .set("arch", os.getArch())
               .set("available_processors", os.getAvailableProcessors())
               .set("system_load_average", os.getSystemLoadAverage()));
         return Json.array(textResult(result.toString()));
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
      Json arguments = params.at("arguments");

      // Find the matching prompt
      McpPrompt prompt = PROMPTS.stream()
            .filter(p -> p.name().equals(name))
            .findFirst()
            .orElse(null);

      if (prompt == null) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.METHOD_NOT_FOUND)
                     .set("message", "Prompt not found: " + name));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Extract arguments
      String topic = arguments.has("topic") ? arguments.at("topic").asString() : null;
      String context = arguments.has("context") ? arguments.at("context").asString() : null;

      // Validate required arguments
      if (topic == null || topic.isEmpty()) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.INVALID_PARAMS)
                     .set("message", "Required argument 'topic' is missing"));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Build the prompt message
      McpPromptMessage message = buildDocumentationPromptMessage(topic, context);

      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("messages", Json.array()
                        .add(message.toJson())
                  )
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpPromptsList(RestRequest request, Json json) {
      Json prompts = Json.array();
      for (McpPrompt prompt : PROMPTS) {
         prompts.add(prompt.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("prompts", prompts)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpCompletionComplete(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      Json argument = params.at("argument");
      String argumentName = argument.at("name").asString();
      Json ref = params.at("ref");
      String refType = ref.at("type").asString();

      // Only support completion for prompts
      if (!"ref/prompt".equals(refType)) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.METHOD_NOT_FOUND)
                     .set("message", "Completion not supported for: " + refType));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Extract prompt name from URI (e.g., "prompt://find-documentation-guided")
      String refName = ref.at("name").asString();

      // Only provide completions for the guided prompt and topic argument
      if ("find-documentation-guided".equals(refName) && "topic".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "cache configuration").set("label", "Cache Configuration"))
               .add(Json.object().set("value", "cross-site replication").set("label", "Cross-Site Replication"))
               .add(Json.object().set("value", "Hot Rod client").set("label", "Hot Rod Client"))
               .add(Json.object().set("value", "query API").set("label", "Query API"))
               .add(Json.object().set("value", "REST API").set("label", "REST API"))
               .add(Json.object().set("value", "persistence").set("label", "Persistence"))
               .add(Json.object().set("value", "security").set("label", "Security"))
               .add(Json.object().set("value", "clustering").set("label", "Clustering"))
               .add(Json.object().set("value", "transactions").set("label", "Transactions"))
               .add(Json.object().set("value", "server management").set("label", "Server Management"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // No completions for other prompts or arguments
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("completion", Json.object()
                        .set("values", Json.array())
                        .set("total", 0)
                        .set("hasMore", false)
                  )
            );
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
                              // .set("logging", Json.object()) // Enable when we support logging
                              .set("tools", Json.object()
                                    .set("listChanged", false) //indicates whether the server will emit notifications when the list of available tools changes.
                              )
                              .set("prompts", Json.object()
                                    .set("listChanged", false) // whether the server will emit notifications when the list of available prompts changes.
                              )
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
      Json resources = Json.array();
      for (McpResource resource : RESOURCES) {
         resources.add(resource.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("resources", resources)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpResourcesTemplatesList(RestRequest request, Json json) {
      Json resourceTemplates = Json.array();
      for (McpResourceTemplate template : RESOURCE_TEMPLATES) {
         resourceTemplates.add(template.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("resourceTemplates", resourceTemplates)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpResourcesRead(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      String uri = params.at("uri").asString();

      // Check if this is a log resource request
      if (uri != null && uri.startsWith("infinispan+logs://")) {
         return handleLogResourceRead(request, json, uri);
      }

      // For other resource types, return not supported
      return unimplemented(request, json, "Resource type not supported: " + uri);
   }

   private CompletionStage<RestResponse> handleLogResourceRead(RestRequest request, Json json, String uri) {
      // Check ADMIN permission for log access
      try {
         invocationHelper.getRestCacheManager().getAuthorizer()
               .checkPermission(AuthorizationPermission.ADMIN);
      } catch (SecurityException e) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.FORBIDDEN)
                     .set("message", "Admin permission required to access log resources"));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      return CompletableFuture.supplyAsync(() -> {
         try {
            // Parse URI: infinispan+logs://server?lines=200
            java.net.URI parsedUri = java.net.URI.create(uri);
            String logType = parsedUri.getHost();
            Map<String, String> queryParams = parseQueryParameters(parsedUri.getQuery());

            // Get lines parameter (default: 200, max: 10000)
            int lines = 200;
            if (queryParams.containsKey("lines")) {
               try {
                  lines = Integer.parseInt(queryParams.get("lines"));
                  if (lines < 1) {
                     lines = 1;
                  } else if (lines > 10000) {
                     lines = 10000;
                  }
               } catch (NumberFormatException e) {
                  throw new IllegalArgumentException("Invalid lines parameter: " + queryParams.get("lines"));
               }
            }

            // Get log file path
            String logPath = System.getProperty("infinispan.server.log.path");
            if (logPath == null) {
               throw new IllegalStateException("Log path not configured (infinispan.server.log.path property not set)");
            }

            String fileName = getLogFileName(logType);
            java.nio.file.Path logFile = java.nio.file.Paths.get(logPath, fileName);

            // Read last N lines
            String content = readLastLines(logFile, lines);

            // Build MCP response
            Json response = rpcResponse(json)
                  .set(McpConstants.RESULT, Json.object()
                        .set("contents", Json.array()
                              .add(Json.object()
                                    .set("uri", uri)
                                    .set("mimeType", "text/plain")
                                    .set("text", content)
                              )
                        )
                  );

            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.OK)
                  .build();

         } catch (IllegalArgumentException e) {
            // Invalid log type or parameters
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.INVALID_PARAMS)
                        .set("message", e.getMessage()));
            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.BAD_REQUEST)
                  .build();
         } catch (java.io.IOException e) {
            // File read error
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.INTERNAL_ERROR)
                        .set("message", "Error reading log file: " + e.getMessage()));
            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                  .build();
         } catch (Exception e) {
            // Unexpected error
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.INTERNAL_ERROR)
                        .set("message", "Unexpected error: " + e.getMessage()));
            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                  .build();
         }
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> mcpResourcesUnubscribe(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/unsubscribe not supported");
   }

   private CompletionStage<RestResponse> mcpResourcesSubscribe(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/subscribe not supported");
   }

   private CompletionStage<RestResponse> mcpNotificationsInitialized(RestRequest request, Json json) {
      // Notifications have no id and expect no response body per the MCP spec
      return CompletableFuture.completedFuture(invocationHelper.newResponse(request).build());
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
         Json content = t != null ? Json.array(textResult(t.getMessage())) : j;
         response.set(McpConstants.RESULT, Json.object()
               .set("isError", t != null)
               .set("content", content)
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
      Json configJson = args.at("configuration");
      Configuration configuration;
      if (configJson != null && configJson.isString()) {
         ConfigurationBuilderHolder holder = invocationHelper.getParserRegistry().parse(configJson.asString(), MediaType.APPLICATION_JSON);
         ConfigurationBuilder cfgBuilder = holder.getCurrentConfigurationBuilder() != null
               ? holder.getCurrentConfigurationBuilder()
               : holder.getNamedConfigurationBuilders().values().iterator().next();
         configuration = cfgBuilder.build(SecurityActions.getCacheManagerConfiguration(invocationHelper.getRestCacheManager().getInstance()));
      } else {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.clustering().cacheMode(CacheMode.DIST_SYNC).encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
         configuration = builder.build();
      }
      return CompletableFuture.supplyAsync(() -> {
         admin.createCache(cacheName, configuration);
         return Json.array();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> deleteCache(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      EmbeddedCacheManagerAdmin admin = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      return CompletableFuture.supplyAsync(() -> {
         admin.removeCache(cacheName);
         return Json.array();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      MediaType requestedMediaType = args.has("mediaType")
            ? MediaType.fromString(args.at("mediaType").asString())
            : MediaType.TEXT_PLAIN;
      return invocationHelper.getRestCacheManager()
            .getInternalEntry(cacheName, key, MediaType.TEXT_PLAIN, requestedMediaType, request)
            .thenApply(entry -> {
               Object value = entry.getValue();
               String text = value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : value.toString();
               return Json.array(textResult(text));
            });
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
      MediaType valueMediaType = args.has("mediaType")
            ? MediaType.fromString(args.at("mediaType").asString())
            : MediaType.TEXT_PLAIN;
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, MediaType.TEXT_PLAIN, valueMediaType, request);
      Configuration config = SecurityActions.getCacheConfiguration(cache);
      final Metadata metadata = CacheOperationsHelper.createMetadata(config, lifespan, maxidle);
      return cache.putAsync(key, value, metadata).thenApply(__ -> Json.array());
   }

   private CompletionStage<Json> setCacheValues(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      Json entries = Json.read(args.at("entries").asString());
      Long lifespan = args.has("lifespan") ? args.at("lifespan").asLong() : null;
      Long maxidle = args.has("maxIdle") ? args.at("maxIdle").asLong() : null;
      return CompletableFuture.supplyAsync(() -> {
         int count = 0;
         for (Json entry : entries.asJsonList()) {
            String key = entry.at("key").asString();
            String value = entry.at("value").asString();
            MediaType valueMediaType = entry.has("mediaType")
                  ? MediaType.fromString(entry.at("mediaType").asString())
                  : MediaType.TEXT_PLAIN;
            AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, MediaType.TEXT_PLAIN, valueMediaType, request);
            Configuration config = SecurityActions.getCacheConfiguration(cache);
            Metadata metadata = CacheOperationsHelper.createMetadata(config, lifespan, maxidle);
            cache.put(key, value, metadata);
            count++;
         }
         return Json.array(textResult("Successfully set " + count + " entries"));
      }, invocationHelper.getExecutor());
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
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, request);
      RemoteQueryManager remoteQueryManager = SecurityActions.getCacheComponentRegistry(cache).getComponent(RemoteQueryManager.class);
      return CompletableFuture.supplyAsync(() -> {
         try {
            byte[] queryResultBytes = remoteQueryManager.executeQuery(query, Map.of(), 0, 100, 10000, cache, MediaType.APPLICATION_JSON, false);
            String result = new String(queryResultBytes, java.nio.charset.StandardCharsets.UTF_8);
            return Json.array(textResult(result));
         } catch (Exception e) {
            return Json.array(textResult("Error executing query: " + e.getMessage()));
         }
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> createCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      CounterType type = CounterType.valueOf(args.at("type").asString().toUpperCase());
      CounterConfiguration.Builder builder = CounterConfiguration.builder(type);
      if (args.has("initialValue")) {
         builder.initialValue(args.at("initialValue").asLong());
      }
      if (args.has("storage")) {
         builder.storage(Storage.valueOf(args.at("storage").asString().toUpperCase()));
      }
      if (args.has("upperBound")) {
         builder.upperBound(args.at("upperBound").asLong());
      }
      if (args.has("lowerBound")) {
         builder.lowerBound(args.at("lowerBound").asLong());
      }
      return invocationHelper.getCounterManager()
            .defineCounterAsync(counterName, builder.build())
            .thenApply(created -> {
               if (created) {
                  return Json.array(textResult("Counter '" + counterName + "' created successfully"));
               }
               return Json.array(textResult("Counter '" + counterName + "' already exists"));
            });
   }

   private CompletionStage<Json> getCounterNames(RestRequest request, Json args) {
      Collection<String> counterNames = invocationHelper.getCounterManager().getCounterNames();
      return CompletableFuture.completedFuture(Json.array(textResult(Json.make(counterNames).toString())));
   }

   private CompletionStage<Json> getCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getOrCreateAsync(counterName)
            .thenCompose(InternalCounterAdmin::value)
            .thenApply(v -> Json.array(textResult(v.toString())));
   }

   private CompletionStage<Json> incrementCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getOrCreateAsync(counterName)
            .thenCompose(counter -> {
               if (counter.isWeakCounter()) {
                  return counter.asWeakCounter().increment().thenApply(__ -> Json.array());
               }
               return counter.asStrongCounter().incrementAndGet()
                     .thenApply(v -> Json.array(textResult(v.toString())));
            });
   }

   private CompletionStage<Json> decrementCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getOrCreateAsync(counterName)
            .thenCompose(counter -> {
               if (counter.isWeakCounter()) {
                  return counter.asWeakCounter().decrement().thenApply(__ -> Json.array());
               }
               return counter.asStrongCounter().decrementAndGet()
                     .thenApply(v -> Json.array(textResult(v.toString())));
            });
   }

   /**
    * Builds a prompt message for documentation search.
    */
   private McpPromptMessage buildDocumentationPromptMessage(String topic, String context) {
      StringBuilder messageText = new StringBuilder();
      messageText.append("Search the Infinispan documentation for information about: ").append(topic);

      if (context != null && !context.isEmpty()) {
         messageText.append("\n\nAdditional context: ").append(context);
      }

      messageText.append("""
                  Documentation structure, list of topics and relative URL to search for:
                  - Configuring caches: https://infinispan.org/docs/stable/titles/configuring/configuring.html
                  - Encoding and marshalling data: https://infinispan.org/docs/stable/titles/encoding/encoding.html
                  - Querying caches: https://infinispan.org/docs/stable/titles/query/query.html
                  - Embedded: https://infinispan.org/docs/stable/titles/embedding/embedding.html
                  - Hot Rod client: https://infinispan.org/docs/stable/titles/hotrod_java/hotrod_java.html
                  - REST API: https://infinispan.org/docs/stable/titles/rest/rest.html
                  - Redis clients: https://infinispan.org/docs/stable/titles/resp/resp-endpoint.html
                  - Memcached clients: https://infinispan.org/docs/stable/titles/memcached/memcached.html
                  - Hibernate ORM cache provider: https://infinispan.org/docs/stable/titles/hibernate/hibernate.html
                  - Quarkus: https://quarkus.io/guides/infinispan-client
                  - Spring Boot: https://infinispan.org/docs/stable/titles/spring_boot/starter.html
                  - Server: https://infinispan.org/docs/stable/titles/server/server.html
                  - Operator: https://infinispan.org/docs/infinispan-operator/main/operator.html
                  - Helm Chart: https://infinispan.org/docs/helm-chart/main/helm-chart.html
                  - Ansible Collection: https://github.com/ansible-middleware/infinispan
                  - Command-Line Interface: https://infinispan.org/docs/stable/titles/cli/cli.html
                  - Planning and Tuning: https://infinispan.org/docs/stable/titles/tuning/tuning.html
                  - Cross-site replication: https://infinispan.org/docs/stable/titles/xsite/xsite.html
                  - Quarkus Langchain Extension: https://docs.quarkiverse.io/quarkus-langchain4j/dev/infinispan-store.html
                  - Langchain: https://python.langchain.com/docs/integrations/vectorstores/infinispanvs
                  - Langchain4j: https://docs.langchain4j.dev/integrations/embedding-stores/infinispan
                  - Vert.x Cluster Manager: https://vertx.io/docs/vertx-infinispan/java/
                  - Vert.x Web Sessions: https://how-to.vertx.io/web-session-infinispan-howto/
                  - Keycloak: https://www.keycloak.org/server/caching
                  - Apache Camel: https://camel.apache.org/components/latest/infinispan-component.html
                  - Wildfly: https://www.wildfly.org/

                  Please search the appropriate documentation URL for the provided topic and provide:
                  1. A brief summary of the topic
                  2. Direct links to relevant documentation pages
                  3. Code examples if available
                  4. Related topics that might be helpful""");

      return new McpPromptMessage("user", messageText.toString());
   }

   /**
    * Parses query parameters from a URI query string.
    * Example: "lines=200&level=ERROR" -> Map{lines=200, level=ERROR}
    */
   private Map<String, String> parseQueryParameters(String query) {
      if (query == null || query.isEmpty()) {
         return Map.of();
      }
      Map<String, String> params = new java.util.HashMap<>();
      for (String param : query.split("&")) {
         String[] pair = param.split("=", 2);
         if (pair.length == 2) {
            params.put(pair[0], pair[1]);
         }
      }
      return params;
   }

   /**
    * Maps log type to actual log file name.
    */
   private String getLogFileName(String logType) {
      return switch (logType == null ? "server" : logType) {
         case "server" -> "server.log";
         case "audit" -> "audit.log";
         case "rest-access" -> "rest-access.log";
         case "hotrod-access" -> "hotrod-access.log";
         case "memcached-access" -> "memcached-access.log";
         case "resp-access" -> "resp-access.log";
         case "gc" -> "gc.log";
         default -> throw new IllegalArgumentException("Unknown log type: " + logType);
      };
   }

   /**
    * Reads the last N lines from a file efficiently using reverse file reading.
    * This approach minimizes memory usage for large files.
    */
   private String readLastLines(java.nio.file.Path file, int maxLines) throws java.io.IOException {
      if (!java.nio.file.Files.exists(file)) {
         return ""; // Return empty string if file doesn't exist yet
      }

      long fileSize = java.nio.file.Files.size(file);
      if (fileSize == 0) {
         return "";
      }

      // For small files, just read the whole thing
      if (fileSize < 8192) {
         return java.nio.file.Files.readString(file);
      }

      // For larger files, read backwards in chunks
      java.util.List<String> lines = new java.util.ArrayList<>();
      try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file.toFile(), "r")) {
         long pos = fileSize - 1;
         StringBuilder currentLine = new StringBuilder();
         int chunkSize = 8192;
         byte[] buffer = new byte[chunkSize];

         while (pos >= 0 && lines.size() < maxLines) {
            int bytesToRead = (int) Math.min(chunkSize, pos + 1);
            pos = pos - bytesToRead + 1;
            raf.seek(pos);
            raf.readFully(buffer, 0, bytesToRead);

            // Process bytes in reverse
            for (int i = bytesToRead - 1; i >= 0; i--) {
               char c = (char) buffer[i];
               if (c == '\n') {
                  if (!currentLine.isEmpty()) {
                     lines.add(currentLine.reverse().toString());
                     currentLine = new StringBuilder();
                     if (lines.size() >= maxLines) {
                        break;
                     }
                  }
               } else if (c != '\r') {
                  currentLine.append(c);
               }
            }
            pos--;
         }

         // Add any remaining content as the first line
         if (!currentLine.isEmpty() && lines.size() < maxLines) {
            lines.add(currentLine.reverse().toString());
         }
      }

      // Reverse the list to get correct order (oldest to newest)
      java.util.Collections.reverse(lines);
      return String.join("\n", lines);
   }
}
