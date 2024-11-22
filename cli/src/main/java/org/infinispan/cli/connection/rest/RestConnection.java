package org.infinispan.cli.connection.rest;

import static org.infinispan.cli.logging.Messages.MSG;
import static org.infinispan.cli.util.TransformingIterable.SINGLETON_MAP_VALUE;
import static org.infinispan.commons.internal.InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.ContainerResource;
import org.infinispan.cli.resources.ContainersResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.cli.util.JsonReaderIterable;
import org.infinispan.cli.util.TransformingIterable;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient.ResultType;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.ServerConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestConnection implements Connection, Closeable {
   private final RestClientConfigurationBuilder builder;
   private Resource activeResource;
   private MediaType encoding = MediaType.TEXT_PLAIN;
   private Collection<String> availableConfigurations;
   private Collection<String> availableContainers;
   private Collection<String> availableCaches;
   private Collection<String> clusterMembers;
   private RestClient client;
   private boolean connected;
   private String serverVersion;
   private String serverInfo;
   private List<String> sitesView;
   private String localSite;
   private boolean relayNode;
   private List<String> relayNodes;
   private final Path workingDir;

   public RestConnection(RestClientConfigurationBuilder builder) {
      this.builder = builder;
      this.workingDir = Paths.get(System.getProperty("user.dir", ""));
   }

   @Override
   public String getURI() {
      if (client != null) {
         return client.getConfiguration().toURI();
      } else {
         return null;
      }
   }

   @Override
   public void close() throws IOException {
      Util.close(client);
   }

   @Override
   public void connect() throws IOException {
      client = RestClient.forConfiguration(builder.build());
      AuthenticationConfiguration authentication = client.getConfiguration().security().authentication();
      if (authentication.enabled() && authentication.username() != null && authentication.password() == null && !"Bearer".equals(authentication.mechanism())) {
         throw new AccessDeniedException("");
      }
      connectInternal();
   }

   @Override
   public void connect(String username, String password) throws IOException {
      builder.security().authentication().enable().username(username).password(password);
      client = RestClient.forConfiguration(builder.build());
      connectInternal();
   }

   private void connectInternal() throws IOException {
      serverVersion = (String) parseBody(fetch(() -> client.server().info()), Map.class).get("version");
      connected = true;
      availableContainers = parseBody(fetch(() -> client.cacheManagers()), List.class);
      activeResource = Resource.getRootResource(this)
            .getChild(ContainersResource.NAME, availableContainers.iterator().next());
      refreshServerInfo();
   }

   private RestResponse fetch(Supplier<CompletionStage<RestResponse>> responseFutureSupplier) throws IOException {
      return fetch(responseFutureSupplier.get());
   }

   private RestResponse fetch(CompletionStage<RestResponse> responseFuture) throws IOException {
      try {
         return responseFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IOException(e);
      } catch (ExecutionException e) {
         throw MSG.connectionFailed(e.getMessage());
      } catch (TimeoutException e) {
         throw new IOException(e);
      }
   }

   private Map<String, List<String>> parseHeaders(RestResponse response) throws IOException {
      response = handleResponseStatus(response);
      if (response != null) {
         return response.headers();
      } else {
         return Collections.emptyMap();
      }
   }

   private <T> T parseBody(RestResponse response, Class<T> returnClass) throws IOException {
      response = handleResponseStatus(response);
      if (response != null) {
         if (returnClass == InputStream.class) {
            return (T) response.bodyAsStream();
         } else if (returnClass == String.class) {
            if (MediaType.APPLICATION_JSON.equals(response.contentType())) {
               Json json = Json.read(response.body());
               return (T) json.toPrettyString();
            } else {
               return (T) response.body();
            }
         } else {
            if (returnClass == Map.class) {
               return (T) Json.read(response.body()).asMap();
            }
            if (returnClass == List.class) {
               return (T) Json.read(response.body()).asList();
            }
         }
      }
      return null;
   }

   private RestResponse handleResponseStatus(RestResponse response) throws IOException {
      switch (response.status()) {
         case 200:
         case 201:
         case 202:
            return response;
         case 204:
            return null;
         case 401:
            throw MSG.unauthorized(response.body());
         case 403:
            throw MSG.forbidden(response.body());
         case 404:
            throw MSG.notFound(response.body());
         default:
            throw MSG.error(response.body());
      }
   }

   @Override
   public MediaType getEncoding() {
      return encoding;
   }

   @Override
   public void setEncoding(MediaType encoding) {
      this.encoding = encoding;
   }

   @Override
   public String execute(BiFunction<RestClient, Resource, CompletionStage<RestResponse>> op, ResponseMode responseMode) throws IOException {
      RestResponse r = fetch(op.apply(client, activeResource));
      return executeInternal(responseMode, r);
   }

   private String executeInternal(ResponseMode responseMode, RestResponse r) throws IOException {
      StringBuilder sb = new StringBuilder();
      switch (responseMode) {
         case BODY:
            String body = parseBody(r, String.class);
            if (body != null) {
               sb.append(body);
            }
            break;
         case FILE:
            Map<String, List<String>> headers = parseHeaders(r);
            String contentDisposition = headers.get("Content-Disposition").get(0);
            String filename = Util.unquote(contentDisposition.split("filename=")[1]);
            Path file = workingDir.resolve(filename);
            boolean gzip = MediaType.APPLICATION_GZIP_TYPE.equalsIgnoreCase(headers.get("content-type").get(0));

            try (OutputStream os = Files.newOutputStream(file); InputStream is = parseResponseBody(r, gzip)) {
               byte[] buffer = new byte[8 * 1024];
               int bytesRead;
               while ((bytesRead = is.read(buffer)) != -1) {
                  os.write(buffer, 0, bytesRead);
               }
               sb.append(MSG.downloadedFile(filename));
            }
         case QUIET:
            break;
         case HEADERS:
            sb.append(Json.make(parseHeaders(r)).toPrettyString());
            break;
         default:
            throw new IllegalArgumentException(responseMode.name());
      }
      refreshServerInfo();
      return sb.toString();
   }

   private InputStream parseResponseBody(RestResponse r, boolean gzip) throws IOException {
      InputStream is = parseBody(r, InputStream.class);
      return gzip
            ? new GZIPInputStream(Objects.requireNonNull(is), 8 * 1024)
            : is;
   }

   @Override
   public Resource getActiveResource() {
      return activeResource;
   }

   @Override
   public void setActiveResource(Resource resource) {
      this.activeResource = resource;
   }

   @Override
   public ContainerResource getActiveContainer() {
      return activeResource.findAncestor(ContainerResource.class);
   }

   @Override
   public Collection<String> getAvailableCaches() {
      return availableCaches;
   }

   @Override
   public Collection<String> getAvailableContainers() {
      return availableContainers;
   }

   @Override
   public Collection<String> getAvailableCounters() throws IOException {
      return parseBody(fetch(() -> client.counters()), List.class);
   }

   @Override
   public Collection<String> getAvailableCacheConfigurations() {
      return availableConfigurations;
   }

   @Override
   public Collection<String> getAvailableSchemas() throws IOException {
      TransformingIterable<Map<String, String>, String> i = new TransformingIterable<>(getCacheKeys(PROTOBUF_METADATA_CACHE_NAME), SINGLETON_MAP_VALUE);
      List<String> list = new ArrayList<>();
      i.forEach(list::add);
      return list;
   }

   @Override
   public Collection<String> getAvailableServers() throws IOException {
      return (List<String>) parseBody(fetch(() -> client.container().info()), Map.class).get("cluster_members");
   }

   @Override
   public Collection<String> getAvailableTasks() throws IOException {
      List<Map<String, String>> list = parseBody(fetch(() -> client.tasks().list(ResultType.ALL)), List.class);
      return list.stream().map(i -> i.get("name")).collect(Collectors.toList());
   }

   @Override
   public Collection<String> getAvailableSites(String cache) throws IOException {
      Map<String, String> sites = parseBody(fetch(() -> client.cache(cache).xsiteBackups()), Map.class);
      return sites == null ? Collections.emptyList() : sites.keySet();
   }

   @Override
   public Iterable<Map<String, String>> getCacheKeys(String cache) throws IOException {
      return new JsonReaderIterable(parseBody(fetch(() -> client.cache(cache).keys()), InputStream.class));
   }

   @Override
   public Iterable<Map<String, String>> getCacheKeys(String cache, int limit) throws IOException {
      return new JsonReaderIterable(parseBody(fetch(() -> client.cache(cache).keys(limit)), InputStream.class));
   }

   @Override
   public Iterable<Map<String, String>> getCacheEntries(String cache, int limit, boolean metadata) throws IOException {
      return new JsonReaderIterable(parseBody(fetch(() -> client.cache(cache).entries(limit, metadata)), InputStream.class));
   }

   @Override
   public Iterable<String> getCounterValue(String counter) throws IOException {
      return Collections.singletonList(parseBody(fetch(() -> client.counter(counter).get()), String.class));
   }

   @Override
   public Collection<String> getRoles() throws IOException {
      return parseBody(fetch(() -> client.security().listRoles(null)), List.class);
   }

   @Override
   public boolean isConnected() {
      return connected;
   }

   @Override
   public String describeContainer() throws IOException {
      return parseBody(fetch(() -> client.container().info()), String.class);
   }

   @Override
   public String describeCache(String cache) throws IOException {
      return parseBody(fetch(() -> client.cache(cache).configuration()), String.class);
   }

   @Override
   public String describeKey(String cache, String key) throws IOException {
      Map<String, List<String>> headers = parseHeaders(fetch(() -> client.cache(cache).head(key)));
      return Json.make(headers).toPrettyString();
   }

   @Override
   public String describeConfiguration(String counter) {
      return null; // TODO
   }

   @Override
   public String describeCounter(String counter) throws IOException {
      return parseBody(fetch(() -> client.counter(counter).configuration()), String.class);
   }

   @Override
   public String describeTask(String taskName) throws IOException {
      List<Map<String, Object>> list = parseBody(fetch(() -> client.tasks().list(ResultType.ALL)), List.class);
      Optional<Map<String, Object>> task = list.stream().filter(i -> taskName.equals(i.get("name"))).findFirst();
      return task.map(Object::toString).orElseThrow(() -> MSG.noSuchResource(taskName));
   }

   @Override
   public Collection<String> getAvailableLogAppenders() throws IOException {
      Map<String, Object> map = parseBody(fetch(() -> client.server().logging().listAppenders()), Map.class);
      return map.keySet();
   }

   @Override
   public Collection<String> getAvailableLoggers() throws IOException {
      List<Map<String, Object>> list = parseBody(fetch(() -> client.server().logging().listLoggers()), List.class);
      return list.stream().map(i -> i.get("name").toString()).collect(Collectors.toList());
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

   @Override
   public Collection<String> getBackupNames() throws IOException {
      return parseBody(fetch(client.container().getBackupNames()), List.class);
   }

   @Override
   public Collection<String> getSitesView() {
      return sitesView;
   }

   @Override
   public String getLocalSiteName() {
      return localSite;
   }

   @Override
   public boolean isRelayNode() {
      return relayNode;
   }

   @Override
   public Collection<String> getRelayNodes() {
      return relayNodes;
   }

   @Override
   public Collection<String> getConnectorNames() throws IOException {
      return parseBody(fetch(client.server().connectorNames()), List.class);
   }

   @Override
   public Collection<String> getDataSourceNames() throws IOException {
      return parseBody(fetch(client.server().dataSourceNames()), List.class);
   }

   @Override
   public Collection<String> getCacheConfigurationAttributes(String name) {
      try {
         return name == null ? Collections.emptyList() : parseBody(fetch(client.cache(name).configurationAttributes()), List.class);
      } catch (IOException e) {
         return Collections.emptyList();
      }
   }

   @Override
   public void refreshServerInfo() throws IOException {
      try {
         Map cacheManagerInfo = parseBody(fetch(() -> client.container().info()), Map.class);
         List<Map<String, Object>> definedCaches = (List<Map<String, Object>>) cacheManagerInfo.get("defined_caches");
         availableCaches = new ArrayList<>();
         definedCaches.forEach(m -> availableCaches.add((String) m.get("name")));
         availableCaches.remove(PROTOBUF_METADATA_CACHE_NAME);
         List configurationList = parseBody(fetch(() -> client.container().cacheConfigurations()), List.class);
         availableConfigurations = new ArrayList<>(configurationList.size());
         for (Object item : configurationList) {
            availableConfigurations.add(((Map<String, String>) item).get("name"));
         }

         String nodeAddress = (String) cacheManagerInfo.get("node_address");
         String clusterName = (String) cacheManagerInfo.get("cluster_name");
         localSite = (String) cacheManagerInfo.get("local_site");
         sitesView = new ArrayList<>((Collection<String>) cacheManagerInfo.get("sites_view"));
         Collections.sort(sitesView);
         relayNode = cacheManagerInfo.containsKey("relay_node") ? (boolean) cacheManagerInfo.get("relay_node") : false;
         relayNodes = (List<String>) cacheManagerInfo.get("relay_nodes_address");
         clusterMembers = (Collection<String>) cacheManagerInfo.get("cluster_members");
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

   @Override
   public String getUsername() {
      return builder.build().security().authentication().username();
   }

   RestClientConfigurationBuilder getBuilder() {
      return builder;
   }

   public String toString() {
      return serverInfo;
   }

}
