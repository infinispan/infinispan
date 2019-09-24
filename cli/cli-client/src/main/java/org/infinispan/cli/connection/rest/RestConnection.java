package org.infinispan.cli.connection.rest;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.infinispan.cli.commands.CommandInputLine;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.CachesResource;
import org.infinispan.cli.resources.ContainerResource;
import org.infinispan.cli.resources.ContainersResource;
import org.infinispan.cli.resources.CountersResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.cli.resources.RootResource;
import org.infinispan.cli.util.IterableJsonReader;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.ServerConfiguration;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestConnection implements Connection, Closeable {
   private final RestClientConfigurationBuilder builder;
   private final ObjectMapper mapper;

   private Resource activeResource;

   private MediaType encoding = MediaType.TEXT_PLAIN;
   private Collection<String> availableConfigurations;
   private Collection<String> availableContainers;
   private Collection<String> availableCaches;
   private Collection<String> availableCounters;
   private Collection<String> clusterMembers;
   private RestClient client;
   private boolean connected;
   private String serverVersion;
   private String serverInfo;

   public RestConnection(RestClientConfigurationBuilder builder) {
      this.builder = builder;
      this.mapper = new ObjectMapper();
   }

   @Override
   public void close() throws IOException {
      client.close();
   }

   @Override
   public void connect() throws IOException {
      client = RestClient.forConfiguration(builder.build());
      connectInternal();
   }

   @Override
   public void connect(String username, String password) throws IOException {
      builder.security().authentication().enable().username(username).password(password);
      client = RestClient.forConfiguration(builder.build());
      connectInternal();
   }

   private void connectInternal() throws IOException {
      serverVersion = (String) parseResponse(() -> client.server().info(), Map.class).get("version");
      connected = true;
      availableContainers = parseResponse(() -> client.cacheManagers(), List.class);
      activeResource = Resource.getRootResource(this)
            .getChild(ContainersResource.NAME, availableContainers.iterator().next());
      refreshServerInfo();
   }

   private <T> T parseResponse(Supplier<CompletionStage<RestResponse>> responseSupplier, Class<T> returnClass) throws IOException {
      return parseResponse(responseSupplier.get(), returnClass);
   }

   private <T> T parseResponse(CompletionStage<RestResponse> responseFuture, Class<T> returnClass) throws IOException {
      try {
         RestResponse response = responseFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);
         switch (response.getStatus()) {
            case 200:
               if (returnClass == InputStream.class) {
                  return (T) response.getBodyAsStream();
               } else if (returnClass == String.class) {
                  if (MediaType.APPLICATION_JSON.equals(response.contentType())) {
                     Object object = mapper.readValue(response.getBody(), Object.class);
                     return (T) mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
                  } else {
                     return (T) response.getBody();
                  }
               } else {
                  return mapper.readValue(response.getBody(), returnClass);
               }
            case 204:
               return null;
            case 401:
               throw MSG.unauthorized(response.getBody());
            case 403:
               throw MSG.forbidden(response.getBody());
            case 404:
               throw MSG.notFound(response.getBody());
            default:
               throw MSG.error(response.getBody());
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IOException(e);
      } catch (ExecutionException e) {
         throw MSG.connectionFailed(e.getMessage());
      } catch (TimeoutException e) {
         throw new IOException(e);
      }
   }

   private Resource pathToResource(String path) {
      if (Resource.THIS.equals(path)) {
         return activeResource;
      } else if (Resource.PARENT.equals(path)) {
         return activeResource.getParent();
      } else {
         String[] parts = path.split("/");
         if (parts.length == 0) {
            return activeResource.findAncestor(RootResource.class);
         } else {
            Resource resource = activeResource;
            for (String part : parts) {
               if (part.isEmpty()) {
                  resource = resource.findAncestor(RootResource.class);
               } else {
                  resource = resource.getChild(part);
               }
            }
            return resource;
         }
      }
   }

   @Override
   public String execute(List<CommandInputLine> commands) throws IOException {
      StringBuilder sb = new StringBuilder();
      for (CommandInputLine command : commands) {
         CompletionStage<RestResponse> response = null;
         switch (command.name()) {
            case "cache":
               activeResource = activeResource
                     .findAncestor(ContainerResource.class)
                     .getChild(CachesResource.NAME, command.arg("name"));
               break;
            case "cd":
               String path = command.arg("name");
               activeResource = pathToResource(path);
               break;
            case "clearcache": {
               activeResource
                     .findAncestor(ContainerResource.class)
                     .getChild(CachesResource.NAME)
                     .getChild(command.arg("name"));
               if (command.hasArg("name")) {
                  response = client.cache(command.arg("name")).clear();
               } else {
                  CacheResource resource = activeResource.findAncestor(CacheResource.class);
                  if (resource != null) {
                     response = client.cache(resource.getName()).clear();
                  }
               }
               break;
            }
            case "container": {
               activeResource = activeResource
                     .findAncestor(RootResource.class)
                     .getChild(ContainersResource.NAME, command.arg("name"));
               break;
            }
            case "counter": {
               activeResource = activeResource
                     .findAncestor(ContainerResource.class)
                     .getChild(CountersResource.NAME, command.arg("name"));
               break;
            }
            case "create": {
               switch (command.arg("type")) {
                  case "cache":
                     RestCacheClient cache = client.cache(command.arg("name"));
                     boolean permanent = Boolean.parseBoolean(command.option("permanent"));
                     CacheContainerAdmin.AdminFlag flags = permanent ? CacheContainerAdmin.AdminFlag.PERMANENT : null;
                     if (command.hasArg("template")) {
                        response = cache.createWithTemplate(command.arg("template"), flags);
                     } else {
                        RestEntity entity = entityFromFile(new File(command.arg("file")));
                        response = cache.createWithConfiguration(entity, flags);
                     }
                     break;
                  case "counter":
                     response = client.counter(command.arg("name")).create();
                     break;
               }
               break;
            }
            case "describe": {
               Resource resource = activeResource;
               if (command.hasArg("name")) {
                  resource = pathToResource(command.arg("name"));
               }
               return resource.describe();
            }
            case "drop": {
               switch (command.arg("type")) {
                  case "cache":
                     response = client.cache(command.arg("name")).delete();
                     break;
                  case "counter":
                     response = client.counter(command.arg("name")).delete();
                     break;
               }
               break;
            }
            case "encoding": {
               if (command.hasArg("type")) {
                  encoding = MediaType.fromString(command.arg("type"));
               } else {
                  sb.append(encoding);
               }
               break;
            }
            case "get": {
               RestCacheClient cache;
               if (command.hasArg("cache")) {
                  cache = client.cache(command.arg("cache"));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               response = cache.get(command.arg("key"));
               break;
            }
            case "ls":
               StringJoiner j = new StringJoiner("\n");
               for (String item : activeResource.getChildrenNames()) {
                  j.add(item);
               }
               return j.toString();
            case "query": {
               RestCacheClient cache;
               if (command.hasArg("cache")) {
                  cache = client.cache(command.arg("cache"));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               response = cache.query(command.arg("query"));
               break;
            }
            case "remove": {
               RestCacheClient cache;
               if (command.hasArg("cache")) {
                  cache = client.cache(command.arg("cache"));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               response = cache.remove(command.arg("key"));
               break;
            }
            case "put": {
               RestCacheClient cache;
               if (command.hasArg("cache")) {
                  cache = client.cache(command.arg("cache"));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               RestEntity value;
               MediaType putEncoding = command.hasArg("encoding") ? MediaType.fromString(command.arg("encoding")) : encoding;
               if (command.hasArg("file")) {
                  value = RestEntity.create(putEncoding, new File(command.arg("file")));
               } else {
                  value = RestEntity.create(putEncoding, command.arg("value"));
               }
               response = cache.put(command.arg("key"), value);
               break;
            }
            case "shutdown": {
               response = client.server().stop().thenApply(f -> {
                        try {
                           close();
                        } catch (Exception e) {
                        }
                        return f;
                     }
               );
               break;
            }
            case "stats":
            case "abort":
            case "begin":
            case "end":
            case "evict":
            case "export":
            case "grant":
            case "revoke":
            case "rollback":
            case "site":
            case "start":
            case "upgrade":
            default:
               break;
         }
         if (response != null) {
            String s = parseResponse(response, String.class);
            if (s != null) {
               sb.append(s);
            }
         }
      }

      refreshServerInfo();
      return sb.toString();
   }

   @Override
   public Resource getActiveResource() {
      return activeResource;
   }

   @Override
   public ContainerResource getActiveContainer() {
      return activeResource.findAncestor(ContainerResource.class);
   }

   @Override
   public Collection<String> getAvailableCaches(String container) {
      return availableCaches;
   }

   @Override
   public Collection<String> getAvailableContainers() {
      return availableContainers;
   }

   @Override
   public Collection<String> getAvailableCounters(String container) {
      return availableCounters;
   }

   @Override
   public Collection<String> getAvailableCacheConfigurations(String container) {
      return availableConfigurations;
   }

   @Override
   public Iterable<String> getCacheKeys(String container, String cache) throws IOException {
      CompletionStage<RestResponse> response = client.cache(cache).keys();
      return new IterableJsonReader(parseResponse(response, InputStream.class), s -> s == null || "_value".equals(s));
   }

   @Override
   public boolean isConnected() {
      return connected;
   }

   @Override
   public String describeContainer(String container) throws IOException {
      return parseResponse(client.cacheManager(container).info(), String.class);
   }

   @Override
   public String describeCache(String container, String cache) throws IOException {
      return parseResponse(client.cache(cache).configuration(), String.class);
   }

   @Override
   public String describeConfiguration(String container, String counter) {
      return null; // TODO
   }

   @Override
   public String describeCounter(String container, String counter) throws IOException {
      return parseResponse(client.counter(counter).configuration(), String.class);
   }

   @Override
   public Collection<String> getClusterNodes() {
      return clusterMembers;
   }

   @Override
   public String getConnectionInfo() {
      return serverInfo;
   }

   @Override
   public String getServerVersion() {
      return serverVersion;
   }

   private void refreshServerInfo() throws IOException {
      try {
         ContainerResource container = getActiveContainer();
         String containerName = container.getName();
         Map cacheManagerInfo = parseResponse(() -> client.cacheManager(containerName).info(), Map.class);
         List<Map<String, Object>> definedCaches = (List<Map<String, Object>>) cacheManagerInfo.get("defined_caches");
         availableCaches = new ArrayList<>();
         definedCaches.forEach(m -> availableCaches.add((String) m.get("name")));
         List configurationList = parseResponse(() -> client.cacheManager(containerName).cacheConfigurations(), List.class);
         availableConfigurations = new ArrayList<>(configurationList.size());
         for (Object item : configurationList) {
            availableConfigurations.add(((Map<String, String>) item).get("name"));
         }
         availableCounters = new ArrayList<>();
         String nodeAddress = (String) cacheManagerInfo.get("node_address");
         String clusterName = (String) cacheManagerInfo.get("cluster_name");
         clusterMembers = (Collection<String>)cacheManagerInfo.get("cluster_members");
         if (nodeAddress != null) {
            serverInfo = nodeAddress + "@" + clusterName;
         } else {
            ServerConfiguration serverConfiguration = client.getConfiguration().servers().get(0);
            serverInfo = serverConfiguration.host() + ":" + serverConfiguration.port();
         }
      } catch (IllegalStateException e) {
         // Cannot refresh if there is no container selected
      }
   }

   private RestEntity entityFromFile(File f) throws IOException {
      try (InputStream is = new FileInputStream(f)) {
         int b;
         while ((b = is.read()) > -1) {
            if (b == '{') {
               return RestEntity.create(MediaType.APPLICATION_JSON, f);
            } else if (b == '<') {
               return RestEntity.create(MediaType.APPLICATION_XML, f);
            }
         }
      }
      return RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, f);
   }

   RestClientConfigurationBuilder getBuilder() {
      return builder;
   }

   public String toString() {
      return serverInfo;
   }
}
