package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.security.impl.SubjectACL;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.1
 */
public class LoginResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private final String accessGrantedPath;
   private final String accessDeniedPath;

   public LoginResource(InvocationHelper invocationHelper, String accessGrantedPath, String accessDeniedPath) {
      this.invocationHelper = invocationHelper;
      this.accessGrantedPath = accessGrantedPath;
      this.accessDeniedPath = accessDeniedPath;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/login").withAction("config").anonymous(true).handleWith(this::loginConfiguration)
            .invocation().methods(GET, POST).deprecated().path("/v2/login").withAction("login").handleWith(this::login)
            .invocation().methods(GET).deprecated().path("/v2/login").handleWith(this::login)
            .invocation().methods(GET).path("/v2/user/acl").handleWith(this::acl)

            .create();
   }

   private CompletionStage<RestResponse> acl(RestRequest request) {
      Subject subject = request.getSubject();
      RestCacheManager<Object> rcm = invocationHelper.getRestCacheManager();
      Collection<String> cacheNames = rcm.getCacheNames();
      Json acl = Json.object();
      AuthorizationHelper authorizationHelper = rcm.getAuthorizationHelper();
      SubjectACL globalACL = authorizationHelper.getACL(subject);
      acl.set("global", aclToJson(globalACL));
      Json caches = Json.object();
      acl.set("caches", caches);
      for(String cacheName : cacheNames) {
         Configuration cacheConfiguration = SecurityActions.getCacheConfigurationFromManager(rcm.getInstance(), cacheName);
         SubjectACL cacheACL = authorizationHelper.getACL(subject, cacheConfiguration.security().authorization());
         caches.set(cacheName, aclToJson(cacheACL));
      }
      return asJsonResponseFuture(acl);
   }

   private Json aclToJson(SubjectACL acl) {
      Json array = Json.array();
      for(AuthorizationPermission permission : acl.getPermissions()) {
         array.add(permission.name());
      }
      return array;
   }

   private CompletionStage<RestResponse> loginConfiguration(RestRequest restRequest) {
      Map<String, String> loginConfiguration = invocationHelper.getServer().getLoginConfiguration(invocationHelper.getProtocolServer());
      return asJsonResponseFuture(Json.make(loginConfiguration));
   }

   private CompletionStage<RestResponse> login(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      responseBuilder.status(HttpResponseStatus.TEMPORARY_REDIRECT).header("Location", accessGrantedPath);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }
}
