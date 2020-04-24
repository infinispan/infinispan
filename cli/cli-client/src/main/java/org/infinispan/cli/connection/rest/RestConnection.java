package org.infinispan.cli.connection.rest;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.cli.commands.Abort;
import org.infinispan.cli.commands.Add;
import org.infinispan.cli.commands.Begin;
import org.infinispan.cli.commands.Cache;
import org.infinispan.cli.commands.Cas;
import org.infinispan.cli.commands.Cd;
import org.infinispan.cli.commands.ClearCache;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.commands.CommandInputLine;
import org.infinispan.cli.commands.Container;
import org.infinispan.cli.commands.Counter;
import org.infinispan.cli.commands.Create;
import org.infinispan.cli.commands.Describe;
import org.infinispan.cli.commands.Drop;
import org.infinispan.cli.commands.Encoding;
import org.infinispan.cli.commands.End;
import org.infinispan.cli.commands.Evict;
import org.infinispan.cli.commands.Get;
import org.infinispan.cli.commands.Grant;
import org.infinispan.cli.commands.Logging;
import org.infinispan.cli.commands.Ls;
import org.infinispan.cli.commands.Put;
import org.infinispan.cli.commands.Query;
import org.infinispan.cli.commands.Remove;
import org.infinispan.cli.commands.Reset;
import org.infinispan.cli.commands.Revoke;
import org.infinispan.cli.commands.Rollback;
import org.infinispan.cli.commands.Schema;
import org.infinispan.cli.commands.Server;
import org.infinispan.cli.commands.Shutdown;
import org.infinispan.cli.commands.Site;
import org.infinispan.cli.commands.Start;
import org.infinispan.cli.commands.Stats;
import org.infinispan.cli.commands.Task;
import org.infinispan.cli.commands.Upgrade;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.CacheKeyResource;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.CachesResource;
import org.infinispan.cli.resources.ContainerResource;
import org.infinispan.cli.resources.ContainersResource;
import org.infinispan.cli.resources.CounterResource;
import org.infinispan.cli.resources.CountersResource;
import org.infinispan.cli.resources.Resource;
import org.infinispan.cli.resources.RootResource;
import org.infinispan.cli.util.IterableJsonReader;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestQueryMode;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient.ResultType;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.ServerConfiguration;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.dataconversion.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestConnection implements Connection, Closeable {
   public static String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";
   private final RestClientConfigurationBuilder builder;
   private final ObjectMapper mapper;

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
      } else {
         return null;
      }
   }

   private RestResponse handleResponseStatus(RestResponse response) throws IOException {
      switch (response.getStatus()) {
         case 200:
            return response;
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
   }

   private Resource pathToResource(String path) throws IOException {
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
      ResponseMode responseMode = ResponseMode.BODY;
      for (CommandInputLine command : commands) {
         CompletionStage<RestResponse> response = null;
         switch (command.name()) {
            case Add.CMD: {
               RestCounterClient counter;
               if (command.hasArg(Add.COUNTER)) {
                  counter = client.counter(command.arg(Add.COUNTER));
               } else {
                  counter = client.counter(activeResource.findAncestor(CounterResource.class).getName());
               }
               response = counter.add(command.longOption(Add.DELTA));
               if (command.boolOption(CliCommand.QUIET)) {
                  responseMode = ResponseMode.QUIET;
               }
               break;
            }
            case Cache.CMD:
               activeResource = activeResource
                     .findAncestor(ContainerResource.class)
                     .getChild(CachesResource.NAME, command.arg(CliCommand.NAME));
               break;
            case Cas.CMD: {
               RestCounterClient counter;
               if (command.hasArg(Cas.COUNTER)) {
                  counter = client.counter(command.arg(Cas.COUNTER));
               } else {
                  counter = client.counter(activeResource.findAncestor(CounterResource.class).getName());
               }
               if (command.boolOption(CliCommand.QUIET)) {
                  response = counter.compareAndSet(command.longOption(Cas.EXPECT), command.longOption(Cas.VALUE));
               } else {
                  response = counter.compareAndSwap(command.longOption(Cas.EXPECT), command.longOption(Cas.VALUE));
               }
               break;
            }
            case Cd.CMD:
               String path = command.arg(CliCommand.PATH);
               Resource rPath = pathToResource(path);
               if (!(rPath instanceof CacheKeyResource)) {
                  activeResource = rPath;
               }
               break;
            case ClearCache.CMD: {
               if (command.hasArg(CliCommand.NAME)) {
                  activeResource
                        .findAncestor(ContainerResource.class)
                        .getChild(CachesResource.NAME)
                        .getChild(command.arg(CliCommand.NAME));
                  response = client.cache(command.arg(CliCommand.NAME)).clear();
               } else {
                  CacheResource resource = activeResource.findAncestor(CacheResource.class);
                  if (resource != null) {
                     response = client.cache(resource.getName()).clear();
                  }
               }
               break;
            }
            case Container.CMD: {
               activeResource = activeResource
                     .findAncestor(RootResource.class)
                     .getChild(ContainersResource.NAME, command.arg(CliCommand.NAME));
               break;
            }
            case Counter.CMD: {
               activeResource = activeResource
                     .findAncestor(ContainerResource.class)
                     .getChild(CountersResource.NAME, command.arg(CliCommand.NAME));
               break;
            }
            case Create.CMD: {
               switch (command.arg(Create.TYPE)) {
                  case Create.Cache.CMD: {
                     RestCacheClient cache = client.cache(command.arg(Create.NAME));
                     boolean vltl = command.boolOption(Create.Cache.VOLATILE);
                     AdminFlag flags[] = vltl ? new AdminFlag[]{AdminFlag.VOLATILE} : new AdminFlag[]{};
                     if (command.hasArg(Create.Cache.TEMPLATE)) {
                        response = cache.createWithTemplate(command.arg(Create.Cache.TEMPLATE), flags);
                     } else {
                        RestEntity entity = entityFromFile(new File(command.arg(Create.Cache.FILE)));
                        response = cache.createWithConfiguration(entity, flags);
                     }
                     break;
                  }
                  case Create.Counter.CMD: {
                     ObjectNode counter = mapper.createObjectNode();
                     ObjectNode node = counter
                           .putObject(command.option(Create.Counter.COUNTER_TYPE) + "-counter")
                           .put(Create.Counter.INITIAL_VALUE, command.longOption(Create.Counter.INITIAL_VALUE))
                           .put(Create.Counter.CONCURRENCY_LEVEL, command.intOption(Create.Counter.CONCURRENCY_LEVEL))
                           .put(Create.Counter.STORAGE, command.option(Create.Counter.STORAGE));
                     if (command.hasOption(Create.Counter.UPPER_BOUND)) {
                        node.put(Create.Counter.UPPER_BOUND, command.longOption(Create.Counter.UPPER_BOUND));
                     }
                     if (command.hasOption(Create.Counter.LOWER_BOUND)) {
                        node.put(Create.Counter.LOWER_BOUND, command.longOption(Create.Counter.LOWER_BOUND));
                     }
                     response = client.counter(command.arg(CliCommand.NAME)).create(RestEntity.create(MediaType.APPLICATION_JSON, counter.toString()));
                     break;
                  }
               }
               break;
            }
            case Describe.CMD: {
               Resource resource = activeResource;
               if (command.hasArg(CliCommand.NAME)) {
                  resource = pathToResource(command.arg(CliCommand.NAME));
               }
               return resource.describe();
            }
            case Drop.CMD: {
               switch (command.arg(CliCommand.TYPE)) {
                  case Drop.Cache.CMD:
                     response = client.cache(command.arg(CliCommand.NAME)).delete();
                     break;
                  case Drop.Counter.CMD:
                     response = client.counter(command.arg(CliCommand.NAME)).delete();
                     break;
               }
               break;
            }
            case Encoding.CMD: {
               if (command.hasArg(CliCommand.TYPE)) {
                  encoding = MediaType.fromString(command.arg(CliCommand.TYPE));
               } else {
                  sb.append(encoding);
               }
               break;
            }
            case Get.CMD: {
               RestCacheClient cache;
               if (command.hasArg(CliCommand.CACHE)) {
                  cache = client.cache(command.arg(CliCommand.CACHE));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               response = cache.get(command.arg(CliCommand.KEY));
               break;
            }
            case Ls.CMD: {
               Resource resource = activeResource;
               if (command.hasArg(CliCommand.PATH)) {
                  resource = pathToResource(command.arg(CliCommand.PATH));
               }
               StringJoiner j = new StringJoiner("\n");
               for (String item : resource.getChildrenNames()) {
                  j.add(item);
               }
               return j.toString();
            }
            case Query.CMD: {
               RestCacheClient cache;
               if (command.hasArg(CliCommand.CACHE)) {
                  cache = client.cache(command.arg(CliCommand.CACHE));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               response = cache.query(
                     command.arg(Query.QUERY),
                     command.intOption(Query.MAX_RESULTS),
                     command.intOption(Query.OFFSET),
                     RestQueryMode.valueOf(command.option(Query.QUERY_MODE))
               );
               break;
            }
            case Put.CMD: {
               RestCacheClient cache;
               if (command.hasOption(CliCommand.CACHE)) {
                  cache = client.cache(command.option(CliCommand.CACHE));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               RestEntity value;
               MediaType putEncoding = command.hasOption(Put.ENCODING) ? MediaType.fromString(command.option(Put.ENCODING)) : encoding;
               if (command.hasOption(CliCommand.FILE)) {
                  value = RestEntity.create(putEncoding, new File(command.option(CliCommand.FILE)));
               } else {
                  value = RestEntity.create(putEncoding, command.arg(CliCommand.VALUE));
               }
               if (command.boolOption(Put.IF_ABSENT)) {
                  response = cache.post(command.arg(CliCommand.KEY), value, command.longOption(Put.TTL), command.longOption(Put.MAX_IDLE));
               } else {
                  response = cache.put(command.arg(CliCommand.KEY), value, command.longOption(Put.TTL), command.longOption(Put.MAX_IDLE));
               }
               break;
            }
            case Remove.CMD: {
               RestCacheClient cache;
               if (command.hasArg(CliCommand.CACHE)) {
                  cache = client.cache(command.arg(CliCommand.CACHE));
               } else {
                  cache = client.cache(activeResource.findAncestor(CacheResource.class).getName());
               }
               response = cache.remove(command.arg(CliCommand.KEY));
               break;
            }
            case Reset.CMD: {
               RestCounterClient counter;
               if (command.hasArg(Reset.COUNTER)) {
                  counter = client.counter(command.arg(Reset.COUNTER));
               } else {
                  counter = client.counter(activeResource.findAncestor(CounterResource.class).getName());
               }
               response = counter.reset();
               break;
            }
            case Schema.CMD: {
               RestCacheClient cache = client.cache(PROTOBUF_METADATA_CACHE_NAME);
               if (command.hasArg(CliCommand.FILE)) {
                  RestEntity value = RestEntity.create(MediaType.TEXT_PLAIN, new File(command.arg(CliCommand.FILE)));
                  response = cache.put(command.arg(CliCommand.KEY), value);
               } else {
                  response = cache.get(command.arg(CliCommand.KEY));
               }
               break;
            }
            case Shutdown.CMD: {
               switch (command.arg(Shutdown.TYPE)) {
                  case Shutdown.Server.CMD: {
                     if (command.hasArg(Shutdown.SERVERS)) {
                        response = client.cluster().stop(command.argAs(Shutdown.SERVERS));
                     } else {
                        response = client.server().stop();
                     }
                     break;
                  }
                  case Shutdown.Cluster.CMD: {
                     response = client.cluster().stop();
                     break;
                  }
               }
               break;
            }
            case Site.CMD: {
               RestCacheClient cache = client.cache(command.arg(Site.CACHE));
               switch (command.arg(Site.OP)) {
                  case Site.STATUS: {
                     if (command.hasArg(Site.SITE_NAME)) {
                        response = cache.backupStatus(command.arg(Site.SITE_NAME));
                     } else {
                        response = cache.xsiteBackups();
                     }
                     break;
                  }
                  case Site.BRING_ONLINE: {
                     response = cache.bringSiteOnline(command.arg(Site.SITE_NAME));
                     break;
                  }
                  case Site.TAKE_OFFLINE: {
                     response = cache.takeSiteOffline(command.arg(Site.SITE_NAME));
                     break;
                  }
                  case Site.PUSH_SITE_STATE: {
                     response = cache.pushSiteState(command.arg(Site.SITE_NAME));
                     break;
                  }
                  case Site.CANCEL_PUSH_STATE: {
                     response = cache.cancelPushState(command.arg(Site.SITE_NAME));
                     break;
                  }
                  case Site.CANCEL_RECEIVE_STATE: {
                     response = cache.cancelReceiveState(command.arg(Site.SITE_NAME));
                     break;
                  }
                  case Site.PUSH_SITE_STATUS: {
                     response = cache.pushStateStatus();
                     break;
                  }
                  case Site.CLEAR_PUSH_STATE_STATUS: {
                     response = cache.clearPushStateStatus();
                     break;
                  }
               }
               break;
            }
            case Task.CMD: {
               switch (command.arg(Task.TYPE)) {
                  case Task.Exec.CMD: {
                     response = client.tasks().exec(command.arg(Task.Exec.NAME), command.argAs(Task.Exec.PARAMETERS));
                     break;
                  }
                  case Task.Upload.CMD: {
                     RestEntity value = RestEntity.create(MediaType.TEXT_PLAIN, new File(command.option(CliCommand.FILE)));
                     response = client.tasks().uploadScript(command.arg(Task.Exec.NAME), value);
                     break;
                  }
               }
               break;
            }
            case Stats.CMD: {
               Resource resource = activeResource;
               if (command.hasArg(CliCommand.NAME)) {
                  resource = pathToResource(command.arg(CliCommand.NAME));
               }
               if (resource instanceof CacheResource) {
                  response = client.cache(resource.getName()).stats();
               } else if (resource instanceof ContainerResource) {
                  response = client.cacheManager(resource.getName()).stats();
               } else {
                  String name = resource.getName();
                  throw MSG.invalidResource(name.isEmpty() ? "/" : name);
               }
            }
            case Logging.CMD: {
               switch (command.arg(Logging.TYPE)) {
                  case Logging.Loggers.CMD: {
                     response = client.server().logging().listLoggers();
                     break;
                  }
                  case Logging.Appenders.CMD: {
                     response = client.server().logging().listAppenders();
                     break;
                  }
                  case Logging.Set.CMD: {
                     if (command.hasArg(Logging.Set.APPENDERS)) {
                        List<String> appenders = command.argAs(Logging.Set.APPENDERS);
                        response = client.server().logging().setLogger(command.arg(Logging.NAME), command.option(Logging.Set.LEVEL), appenders.toArray(new String[0]));
                     } else {
                        response = client.server().logging().setLogger(command.arg(Logging.NAME), command.option(Logging.Set.LEVEL));
                     }
                     break;
                  }
                  case Logging.Remove.CMD: {
                     response = client.server().logging().removeLogger(command.arg(Logging.NAME));
                     break;
                  }
               }
               break;
            }
            case Server.CMD: {
               switch (command.arg(Logging.TYPE)) {
                  case Server.Report.CMD: {
                     responseMode = ResponseMode.FILE;
                     response = client.server().report();
                  }
                  break;
               }
               break;
            }
            case Abort.CMD:
            case Begin.CMD:
            case End.CMD:
            case Evict.CMD:
            case Grant.CMD:
            case Revoke.CMD:
            case Rollback.CMD:
            case Start.CMD:
            case Upgrade.CMD:
            default:
               break;
         }
         if (response != null) {
            RestResponse r = fetch(response);
            switch (responseMode) {
               case BODY:
                  String body = parseBody(r, String.class);
                  if (body != null) {
                     sb.append(body);
                  }
                  break;
               case FILE:
                  String contentDisposition = r.headers().get("Content-Disposition").get(0);
                  String filename = contentDisposition.substring(contentDisposition.indexOf('"') + 1, contentDisposition.lastIndexOf('"'));

                  try (OutputStream os = new FileOutputStream(filename); InputStream is = parseBody(r, InputStream.class)) {
                     byte[] buffer = new byte[8 * 1024];
                     int bytesRead;
                     while ((bytesRead = is.read(buffer)) != -1) {
                        os.write(buffer, 0, bytesRead);
                     }
                     sb.append(MSG.downloadedReport(filename));
                  }
               case QUIET:
                  break;
               case HEADERS:
                  sb.append(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(parseHeaders(r)));
                  break;
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
   public Collection<String> getAvailableCounters(String container) throws IOException {
      return parseBody(fetch(() -> client.counters()), List.class);
   }

   @Override
   public Collection<String> getAvailableCacheConfigurations(String container) {
      return availableConfigurations;
   }

   @Override
   public Collection<String> getAvailableSchemas(String container) throws IOException {
      List<String> schemas = new ArrayList<>();
      getCacheKeys(container, PROTOBUF_METADATA_CACHE_NAME).forEach(s -> schemas.add(s));
      return schemas;
   }

   @Override
   public Collection<String> getAvailableServers(String container) throws IOException {
      return (List<String>) parseBody(fetch(() -> client.cacheManager(container).info()), Map.class).get("cluster_members");
   }

   @Override
   public Collection<String> getAvailableTasks(String container) throws IOException {
      List<Map<String, String>> list = parseBody(fetch(() -> client.tasks().list(ResultType.ALL)), List.class);
      return list.stream().map(i -> i.get("name")).collect(Collectors.toList());
   }

   @Override
   public Collection<String> getAvailableSites(String container, String cache) throws IOException {
      CompletionStage<RestResponse> response = client.cache(cache).xsiteBackups();
      return null;
   }

   @Override
   public Iterable<String> getCacheKeys(String container, String cache) throws IOException {
      return new IterableJsonReader(parseBody(fetch(() -> client.cache(cache).keys()), InputStream.class), s -> s == null || "_value".equals(s));
   }

   @Override
   public Iterable<String> getCounterValue(String container, String counter) throws IOException {
      return Collections.singletonList(parseBody(fetch(() -> client.counter(counter).get()), String.class));
   }

   @Override
   public boolean isConnected() {
      return connected;
   }

   @Override
   public String describeContainer(String container) throws IOException {
      return parseBody(fetch(() -> client.cacheManager(container).info()), String.class);
   }

   @Override
   public String describeCache(String container, String cache) throws IOException {
      return parseBody(fetch(() -> client.cache(cache).configuration()), String.class);
   }

   @Override
   public String describeKey(String container, String cache, String key) throws IOException {
      Map<String, List<String>> headers = parseHeaders(fetch(() -> client.cache(cache).head(key)));
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(headers);
   }

   @Override
   public String describeConfiguration(String container, String counter) {
      return null; // TODO
   }

   @Override
   public String describeCounter(String container, String counter) throws IOException {
      return parseBody(fetch(() -> client.counter(counter).configuration()), String.class);
   }

   @Override
   public String describeTask(String container, String taskName) throws IOException {
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

   private void refreshServerInfo() throws IOException {
      try {
         ContainerResource container = getActiveContainer();
         String containerName = container.getName();
         Map cacheManagerInfo = parseBody(fetch(() -> client.cacheManager(containerName).info()), Map.class);
         List<Map<String, Object>> definedCaches = (List<Map<String, Object>>) cacheManagerInfo.get("defined_caches");
         availableCaches = new ArrayList<>();
         definedCaches.forEach(m -> availableCaches.add((String) m.get("name")));
         definedCaches.remove(PROTOBUF_METADATA_CACHE_NAME);
         List configurationList = parseBody(fetch(() -> client.cacheManager(containerName).cacheConfigurations()), List.class);
         availableConfigurations = new ArrayList<>(configurationList.size());
         for (Object item : configurationList) {
            availableConfigurations.add(((Map<String, String>) item).get("name"));
         }

         String nodeAddress = (String) cacheManagerInfo.get("node_address");
         String clusterName = (String) cacheManagerInfo.get("cluster_name");
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

   enum ResponseMode {QUIET, BODY, FILE, HEADERS}
}
