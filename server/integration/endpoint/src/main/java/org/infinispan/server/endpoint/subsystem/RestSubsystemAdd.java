/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.rest.configuration.ExtendedHeaders;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.registry.Resource;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.controller.services.path.PathManagerService;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

/**
 * RestSubsystemAdd.
 *
 * @author Tristan Tarrant
 * @since 5.1
 */
class RestSubsystemAdd extends AbstractAddStepHandler {
   static final RestSubsystemAdd INSTANCE = new RestSubsystemAdd();

   @Override
   protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model)
            throws OperationFailedException {
      // Read the full model
      ModelNode config = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));

      RestAuthMethod restAuthMethod = RestAuthMethod.NONE;
      ModelNode authConfig = null;
      if (config.hasDefined(ModelKeys.AUTHENTICATION) && config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME).isDefined()) {
         authConfig = config.get(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME);
         restAuthMethod = RestAuthMethod.valueOf(RestAuthenticationResource.AUTH_METHOD.resolveModelAttribute(context, authConfig).asString());
      }
      String contextPath = RestConnectorResource.CONTEXT_PATH.resolveModelAttribute(context, config).asString();
      ExtendedHeaders extendedHeaders = ExtendedHeaders.valueOf(RestConnectorResource.EXTENDED_HEADERS.resolveModelAttribute(context, config).asString());

      Set<String> ignoredCaches = Collections.emptySet();
      if (config.hasDefined(ModelKeys.IGNORED_CACHES)) {
         ignoredCaches = config.get(ModelKeys.IGNORED_CACHES).asList()
               .stream().map(ModelNode::asString).collect(Collectors.toSet());
      }
      int maxContentLength = RestConnectorResource.MAX_CONTENT_LENGTH.resolveModelAttribute(context, config).asInt();
      int compressLevel = RestConnectorResource.COMPRESSION_LEVEL.resolveModelAttribute(context, config).asInt();
      List<CorsConfig> corsConfig = getCorsConfig(config);

      // Create the service
      final RestService service = new RestService(getServiceName(config), restAuthMethod, cleanContextPath(contextPath), extendedHeaders, ignoredCaches, maxContentLength, compressLevel, corsConfig);

      // Setup the various dependencies with injectors and install the service
      ServiceBuilder<?> builder = context.getServiceTarget().addService(EndpointUtils.getServiceName(operation, "rest"), service);
      String cacheContainerName = config.hasDefined(ModelKeys.CACHE_CONTAINER) ? config.get(ModelKeys.CACHE_CONTAINER).asString() : null;
      EndpointUtils.addCacheContainerDependency(builder, cacheContainerName, service.getCacheManager());
      EndpointUtils.addCacheDependency(builder, cacheContainerName, null);
      EndpointUtils.addSocketBindingDependency(context, builder, getSocketBindingName(operation), service.getSocketBinding());
      EndpointUtils.addSocketBindingDependency(context, builder, ModelKeys.MANAGEMENT_HTTP, service.getSocketBindingManagementPlain());
      EndpointUtils.addSocketBindingDependency(context, builder, ModelKeys.MANAGEMENT_HTTPS, service.getSocketBindingManagementSecured());

      builder.addDependency(PathManagerService.SERVICE_NAME, PathManager.class, service.getPathManagerInjector());

      if (authConfig != null) {
         if(authConfig.hasDefined(ModelKeys.SECURITY_REALM)) {
            EndpointUtils.addSecurityRealmDependency(builder, RestAuthenticationResource.SECURITY_REALM.resolveModelAttribute(context, authConfig).asString(), service.getAuthenticationSecurityRealm());
         }
      }

      EncryptableSubsystemHelper.processEncryption(context, config, service, builder);
      builder.setInitialMode(ServiceController.Mode.ACTIVE);
      builder.install();
   }

   private static String cleanContextPath(String s) {
      if (s.endsWith("/")) {
         return s.substring(0, s.length() - 1);
      } else {
         return s;
      }
   }

   protected String getSocketBindingName(ModelNode config) {
      return config.hasDefined(ModelKeys.SOCKET_BINDING) ? config.get(ModelKeys.SOCKET_BINDING).asString() : null;
   }

   protected String getServiceName(ModelNode config) {
      return config.hasDefined(ModelKeys.NAME) ? config.get(ModelKeys.NAME).asString() : "";
   }

   @Override
   protected void populateModel(ModelNode source, ModelNode target) throws OperationFailedException {
      populate(source, target);
   }

   private static void populate(ModelNode source, ModelNode target) throws OperationFailedException {
      for(AttributeDefinition attr : ProtocolServerConnectorResource.COMMON_CONNECTOR_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
      for(AttributeDefinition attr : ProtocolServerConnectorResource.COMMON_LIST_CONNECTOR_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
      for(AttributeDefinition attr : RestConnectorResource.REST_ATTRIBUTES) {
         attr.validateAndSet(source, target);
      }
   }

   private List<CorsConfig> getCorsConfig(ModelNode modelNode) {
      List<CorsConfig> corsConfigList = new ArrayList<>();
      if (modelNode.hasDefined(ModelKeys.CORS_RULE)) {
         List<ModelNode> rules = modelNode.get(ModelKeys.CORS_RULE).asList();
         for(ModelNode rule: rules) {
            ModelNode ruleDefinition = rule.get(rule.keys().iterator().next());
            Integer maxAgeSeconds = extractInt(ruleDefinition, ModelKeys.MAX_AGE_SECONDS);
            Boolean allowCredentials = extractBool(ruleDefinition, ModelKeys.ALLOW_CREDENTIALS);
            String[] origins = asArray(ruleDefinition, ModelKeys.ALLOWED_ORIGINS);
            String[] methods = asArray(ruleDefinition, ModelKeys.ALLOWED_METHODS);
            String[] headers = asArray(ruleDefinition, ModelKeys.ALLOWED_HEADERS);
            String[] exposes = asArray(ruleDefinition, ModelKeys.EXPOSE_HEADERS);

            HttpMethod[] httpMethods = Arrays.stream(methods).map(HttpMethod::valueOf).toArray(HttpMethod[]::new);

            CorsConfigBuilder builder;
            if (Arrays.stream(origins).anyMatch(s -> s.equals("*"))) {
               builder = CorsConfigBuilder.forAnyOrigin();
            } else {
               builder = CorsConfigBuilder.forOrigins(origins);
            }
            builder.allowedRequestMethods(httpMethods);
            if (headers.length > 0) builder.allowedRequestHeaders(headers);
            if (exposes.length > 0) builder.exposeHeaders(exposes);
            if (maxAgeSeconds != null) builder.maxAge(maxAgeSeconds);
            if (allowCredentials) builder.allowCredentials();
            corsConfigList.add(builder.build());
         }
      }
      return corsConfigList;
   }

   private String[] asArray(ModelNode node, String listProperty) {
      if (node != null && node.isDefined() && node.hasDefined(listProperty)) {
         return node.get(listProperty).asList().stream()
               .map(ModelNode::asString).toArray(String[]::new);
      }
      return new String[0];
   }

   private Integer extractInt(ModelNode node, String property) {
      if (node != null && node.isDefined() && node.hasDefined(property)) {
         return node.get(property).asInt();
      }
      return null;
   }

   private Boolean extractBool(ModelNode node, String property) {
      return node != null && node.isDefined() && node.hasDefined(property) && node.get(property).asBoolean();
   }

   @Override
   protected boolean requiresRuntimeVerification() {
      return false;
   }
}
