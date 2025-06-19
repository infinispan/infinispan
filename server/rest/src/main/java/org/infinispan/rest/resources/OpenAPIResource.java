package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.framework.InvocationRegistry;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.PathItem;
import org.infinispan.rest.framework.ResourceDescription;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.ResourceManagerImpl;
import org.infinispan.rest.framework.openapi.Components;
import org.infinispan.rest.framework.openapi.Info;
import org.infinispan.rest.framework.openapi.License;
import org.infinispan.rest.framework.openapi.OpenAPIDocument;
import org.infinispan.rest.framework.openapi.Operation;
import org.infinispan.rest.framework.openapi.Parameter;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Path;
import org.infinispan.rest.framework.openapi.Paths;
import org.infinispan.rest.framework.openapi.ResponseContent;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.tasks.TaskManager;

public class OpenAPIResource implements ResourceHandler {

   public static final String OPENAPI_VERSION = "3.1.1";
   private final InvocationHelper invocationHelper;
   private final InvocationRegistry registry;
   private volatile Json openapi = null;

   public OpenAPIResource(InvocationHelper invocationHelper, InvocationRegistry registry) {
      this.invocationHelper = invocationHelper;
      this.registry = registry;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("openapi", "Generates the OpenAPI descriptor")
            .invocation().method(Method.GET).path("/v3/openapi").handleWith(this::createResponse).anonymous(true)
            .response(OK, "OpenAPI descriptor", MediaType.APPLICATION_JSON, Schema.NONE)
            .create();
   }

   private CompletionStage<RestResponse> createResponse(RestRequest request) {
      if (openapi == null) {
         synchronized (this) {
            if (openapi == null) {
               OpenAPIDocument document = generateOpenAPIDocument();
               openapi = document.toJson();
            }
         }
      }

      return CompletableFuture.completedFuture(
            invocationHelper.newResponse(request)
                  .contentType(MediaType.APPLICATION_JSON)
                  .entity(openapi)
                  .status(OK)
                  .build()
      );
   }

   private OpenAPIDocument generateOpenAPIDocument() {
      Set<Path> paths = new TreeSet<>();
      Set<ResourceDescription> resources = new HashSet<>();
      Map<String, Schema> schemas = new HashMap<>();
      registry.traverse((ignore, invocation) -> {
         for (String p : invocation.paths()) {
            if (!p.startsWith("/v3/") && !p.endsWith("/openapi")) continue; // Skip older apis and ourselves
            resources.add(invocation.resourceGroup());
            String name = invocation.name() != null
                  ? invocation.name()
                  : "-";
            List<Parameter> parameters = new ArrayList<>(5);
            for (String param : PathItem.retrieveAllPathVariables(p)) {
               Parameter parameter = new Parameter(param, ParameterIn.PATH, true, Schema.STRING, param);
               parameters.add(parameter);
            }
            parameters.addAll(invocation.parameters());
            if (invocation.requestBody() != null) {
               invocation.requestBody().schemas().values().forEach(schema -> {
                  if (!schema.isPrimitive()) {
                     schemas.put(schema.name(), schema);
                  }
               });
            }
            for (ResponseContent responseContent : invocation.responses().values()) {
               Map<MediaType, Schema> responses = responseContent.responses();
               for (Schema schema : responses.values()) {
                  if (schema != null && !schema.isPrimitive()) {
                     schemas.put(schema.name(), schema);
                  }
               }
            }
            for (Method method : invocation.methods()) {
               String operationId = invocation.operationId();
               if (operationId != null) {
                  if (Character.isUpperCase(operationId.charAt(0))) {
                     operationId = method.name().toLowerCase() + operationId;
                  }
               }
               Operation operation = new Operation(operationId, name, "", invocation.deprecated(), invocation.resourceGroup(), parameters, invocation.requestBody(), invocation.responses().values());
               Path path = new Path("/" + invocationHelper.getContext() + p, method, operation);
               paths.add(path);
            }
         }
      });
      return new OpenAPIDocument(OPENAPI_VERSION,
            new Info("Infinispan REST API", "Infinispan OpenAPI descriptor",
                  new License("Apache 2.0", "https://www.apache.org/licenses/LICENSE-2.0.html"), Version.getVersion()),
            new Paths(paths),
            new Components(schemas),
            resources
      );
   }

   public static void main(String[] args) throws IOException {
      try (DefaultCacheManager cacheManager = new DefaultCacheManager(false)) {
         RestCacheManager<Object> restCacheManager = new RestCacheManager<>(cacheManager, null);
         RestServerConfiguration configuration = new RestServerConfigurationBuilder().build();
         InvocationHelper invocationHelper = new InvocationHelper(null, restCacheManager, configuration, new ServerManagement() {
            @Override
            public ComponentStatus getStatus() {
               return ComponentStatus.INSTANTIATED;
            }

            @Override
            public void serializeConfiguration(ConfigurationWriter writer) {
            }

            @Override
            public void serverStop(List<String> servers) {
            }

            @Override
            public void clusterStop() {

            }

            @Override
            public void containerStop() {

            }

            @Override
            public DefaultCacheManager getCacheManager() {
               return null;
            }

            @Override
            public ServerStateManager getServerStateManager() {
               return null;
            }

            @Override
            public Map<String, String> getLoginConfiguration(ProtocolServer protocolServer) {
               return Map.of();
            }

            @Override
            public Map<String, ProtocolServer> getProtocolServers() {
               return Map.of();
            }

            @Override
            public TaskManager getTaskManager() {
               return null;
            }

            @Override
            public CompletionStage<java.nio.file.Path> getServerReport() {
               return null;
            }

            @Override
            public BackupManager getBackupManager() {
               return null;
            }

            @Override
            public Map<String, DataSource> getDataSources() {
               return Map.of();
            }

            @Override
            public java.nio.file.Path getServerDataPath() {
               return null;
            }

            @Override
            public Map<String, List<Principal>> getUsers() {
               return Map.of();
            }

            @Override
            public CompletionStage<Void> flushSecurityCaches() {
               return null;
            }

            @Override
            public Json overviewReport() {
               return null;
            }

            @Override
            public Json securityOverviewReport() {
               return null;
            }
         }, null);
         ResourceManager resourceManager = new ResourceManagerImpl();
         OpenAPIResource openAPIResource = new OpenAPIResource(invocationHelper, resourceManager.registry());
         resourceManager.registerResource("/", new CacheResourceV3(invocationHelper, null));
         resourceManager.registerResource("/", openAPIResource);
         Json json = openAPIResource.generateOpenAPIDocument().toJson();
         if (args.length == 1) {
            try (PrintStream out = new PrintStream(Files.newOutputStream(java.nio.file.Paths.get(args[0])))) {
               out.println(json.toPrettyString());
            }
         } else {
            System.out.println(json.toPrettyString());
         }
      }
   }
}
