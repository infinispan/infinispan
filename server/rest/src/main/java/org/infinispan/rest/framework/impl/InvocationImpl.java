package org.infinispan.rest.framework.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;

/**
 * @since 10.0
 */
public class InvocationImpl implements Invocation {

   private final Set<Method> methods;
   private final Set<String> paths;
   private final Function<RestRequest, CompletionStage<RestResponse>> handler;
   private final String action;
   private final String name;
   private final boolean anonymous;
   private final boolean deprecated;
   private final AuthorizationPermission permission;
   private final AuditContext auditContext;

   private InvocationImpl(Set<Method> methods, Set<String> paths, Function<RestRequest,
         CompletionStage<RestResponse>> handler, String action, String name, boolean anonymous, AuthorizationPermission permission, boolean deprecated, AuditContext auditContext) {
      this.methods = methods;
      this.paths = paths;
      this.handler = handler;
      this.action = action;
      this.name = name;
      this.anonymous = anonymous;
      this.permission = permission;
      this.deprecated = deprecated;
      this.auditContext = auditContext;
   }

   public String getAction() {
      return action;
   }

   @Override
   public Set<Method> methods() {
      return methods;
   }

   @Override
   public Set<String> paths() {
      return paths;
   }


   @Override
   public String getName() {
      return name;
   }

   @Override
   public Function<RestRequest, CompletionStage<RestResponse>> handler() {
      return handler;
   }

   @Override
   public boolean anonymous() {
      return anonymous;
   }

   @Override
   public AuthorizationPermission permission() {
      return permission;
   }

   @Override
   public AuditContext auditContext() {
      return auditContext;
   }

   @Override
   public boolean deprecated() {
      return deprecated;
   }

   public static class Builder {
      private final Invocations.Builder parent;
      private Set<Method> methods = new HashSet<>();
      private Set<String> paths = new HashSet<>();
      private Function<RestRequest, CompletionStage<RestResponse>> handler;
      private String action = null;
      private String name = null;
      private boolean anonymous;
      private boolean deprecated;
      private AuthorizationPermission permission;
      private AuditContext auditContext;


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

      public Builder auditContext(AuditContext auditContext) {
         this.auditContext = auditContext;
         return this;
      }

      public Builder deprecated() {
         this.deprecated = true;
         return this;
      }

      public Builder withAction(String action) {
         this.action = action;
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
         return new InvocationImpl(methods, paths, handler, action, name, anonymous, permission, deprecated, auditContext);
      }
   }

   @Override
   public String toString() {
      return "InvocationImpl{" +
            "methods=" + methods +
            ", paths=" + paths +
            ", handler=" + handler +
            ", action='" + action + '\'' +
            ", name='" + name + '\'' +
            ", anonymous=" + anonymous +
            ", deprecated=" + deprecated +
            ", permission=" + permission +
            ", auditContext=" + auditContext +
            '}';
   }
}
