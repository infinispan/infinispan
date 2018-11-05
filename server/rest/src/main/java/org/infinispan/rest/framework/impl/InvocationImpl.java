package org.infinispan.rest.framework.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

/**
 * @since 10.0
 */
public class InvocationImpl implements Invocation {

   private final Set<Method> methods;
   private final Set<String> paths;
   private final Function<RestRequest, RestResponse> handler;
   private final String action;
   private final String name;

   private InvocationImpl(Set<Method> methods, Set<String> paths, Function<RestRequest, RestResponse> handler, String action, String name) {
      this.methods = methods;
      this.paths = paths;
      this.handler = handler;
      this.action = action;
      this.name = name;
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
   public Function<RestRequest, RestResponse> handler() {
      return handler;
   }

   public static class Builder {
      private final Invocations.Builder parent;
      private Set<Method> methods = new HashSet<>();
      private Set<String> paths = new HashSet<>();
      private Function<RestRequest, RestResponse> handler;
      private String action = null;
      private String name = null;

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

      public Builder handleWith(Function<RestRequest, RestResponse> handler) {
         this.handler = handler;
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
         return new InvocationImpl(methods, paths, handler, action, name);
      }
   }

}
