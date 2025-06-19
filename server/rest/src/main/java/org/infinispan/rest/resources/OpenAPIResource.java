package org.infinispan.rest.resources;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.InvocationRegistry;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.PathItem;
import org.infinispan.rest.framework.ResourceDescription;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.openapi.Info;
import org.infinispan.rest.framework.openapi.License;
import org.infinispan.rest.framework.openapi.OpenAPIDocument;
import org.infinispan.rest.framework.openapi.Operation;
import org.infinispan.rest.framework.openapi.Parameter;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Path;
import org.infinispan.rest.framework.openapi.Paths;
import org.infinispan.rest.framework.openapi.Schema;

import io.netty.handler.codec.http.HttpResponseStatus;

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
      return new Invocations.Builder("openapi", "Generates the OpenAPI description")
            .invocation().method(Method.GET).path("/v3/openapi").handleWith(this::createResponse).anonymous(true).response(MediaType.APPLICATION_JSON)
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
                  .status(HttpResponseStatus.OK)
                  .build()
      );
   }

   private OpenAPIDocument generateOpenAPIDocument() {
      Set<Path> paths = new TreeSet<>();
      Set<ResourceDescription> resources = new HashSet<>();
      registry.traverse((ignore, invocation) -> {
         for (String p : invocation.paths()) {
            if (!p.startsWith("/v3/")) continue; // Skip older apis
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
            Operation operation = new Operation(name, invocation.deprecated(), invocation.resourceGroup(), parameters, invocation.responses().values());
            for (Method method : invocation.methods()) {
               Path path = new Path("/" + invocationHelper.getContext() + p, method, operation);
               paths.add(path);
            }
         }
      });
      return new OpenAPIDocument(OPENAPI_VERSION,
            new Info("Infinispan REST API", "OpenAPI description of Infinispan REST endpoint",
            new License("Apache 2.0", "https://www.apache.org/licenses/LICENSE-2.0.html"), Version.getVersion()),
            new Paths(paths),
            resources
      );
   }
}
