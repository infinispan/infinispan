package org.infinispan.rest.framework.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.ResourceDescription;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.openapi.Parameter;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.RequestBody;
import org.infinispan.rest.framework.openapi.ResponseContent;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.0
 */
public record InvocationImpl(ResourceDescription resourceGroup, Set<Method> methods, Set<String> paths,
                             Function<RestRequest, CompletionStage<RestResponse>> handler, @Deprecated String action,
                             String operationId,
                             String name, boolean anonymous, AuthorizationPermission permission, boolean deprecated,
                             boolean requireCacheManagerStart,
                             AuditContext auditContext,
                             RequestBody requestBody,
                             Map<HttpResponseStatus, ResponseContent> responses,
                             List<Parameter> parameters) implements Invocation {
   public static class Builder {
      private final Invocations.Builder parent;
      private final Set<Method> methods = new HashSet<>(2);
      private final Set<String> paths = new HashSet<>(2);
      private final List<Parameter> parameters = new ArrayList<>(2);
      private Function<RestRequest, CompletionStage<RestResponse>> handler;
      private String action = null;
      private String name = null;
      private boolean anonymous;
      private boolean deprecated;
      private AuthorizationPermission permission;
      private AuditContext auditContext;
      private boolean requireCacheManagerStart = true;
      private Map<HttpResponseStatus, ResponseContent> responses;
      private RequestBody request;
      private String operationId;

      public Builder method(Method method) {
         this.methods.add(method);
         return this;
      }

      public Builder methods(Method... methods) {
         Collections.addAll(this.methods, methods);
         return this;
      }

      public Builder path(String path) {
         this.paths.add(path);
         return this;
      }

      public Builder name(String name) {
         this.name = name;
         return this;
      }

      public Builder handleWith(Function<RestRequest, CompletionStage<RestResponse>> handler) {
         this.handler = handler;
         return this;
      }

      public Builder anonymous(boolean anonymous) {
         this.anonymous = anonymous;
         return this;
      }

      public Builder anonymous() {
         this.anonymous = true;
         return this;
      }

      public Builder permission(AuthorizationPermission permission) {
         this.permission = permission;
         return this;
      }

      public Builder response(HttpResponseStatus status, String description) {
         return response(status, description, null, null);
      }

      public Builder response(HttpResponseStatus status, String description, MediaType type) {
         return response(status, description, type, Schema.STRING);
      }

      public Builder response(HttpResponseStatus status, String description, MediaType type, Schema schema) {
         if (responses == null) {
            responses = new HashMap<>(2);
         }
         responses.compute(status, (s, m) -> {
            if (m == null) {
               m = new ResponseContent(description, status, new HashMap<>(2));
            }
            if (type != null) {
               m.responses().put(type, schema);
            }
            return m;
         });
         return this;
      }

      public Builder auditContext(AuditContext auditContext) {
         this.auditContext = auditContext;
         return this;
      }

      public Builder deprecated() {
         this.deprecated = true;
         return this;
      }

      @Deprecated
      public Builder withAction(String action) {
         this.action = action;
         return this;
      }

      public Builder requireCacheManagerStart(boolean value) {
         this.requireCacheManagerStart = value;
         return this;
      }

      public Invocations create() {
         return parent.build(this);
      }

      public Builder invocation() {
         return parent.invocation();
      }

      Builder(Invocations.Builder parent) {
         this.parent = parent;
      }

      InvocationImpl build() {
         return new InvocationImpl(parent.description(), methods, paths, handler, action,
               operationId, name, anonymous, permission, deprecated, requireCacheManagerStart, auditContext,
               request, responses == null ? Collections.emptyMap() : responses, parameters);
      }

      public Builder parameter(Enum<?> name, ParameterIn in, boolean required, Schema schema, String description) {
         return parameter(name.toString(), in, required, schema, description);
      }

      public Builder parameter(String name, ParameterIn in, boolean required, Schema schema, String description) {
         this.parameters.add(new Parameter(name, in, required, schema, description));
         return this;
      }

      public Builder request(String description, boolean required, Map<MediaType, Schema> schemas) {
         this.request = new RequestBody(description, required, schemas);
         return this;
      }

      public Builder operationId(String operationId) {
         this.operationId = operationId;
         return this;
      }
   }
}
