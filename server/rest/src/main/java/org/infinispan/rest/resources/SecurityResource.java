package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.MutablePrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.security.impl.SubjectACL;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.1
 */
public class SecurityResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private final String accessGrantedPath;
   private final String accessDeniedPath;
   private final MutablePrincipalRoleMapper principalRoleMapper;

   public SecurityResource(InvocationHelper invocationHelper, String accessGrantedPath, String accessDeniedPath) {
      this.invocationHelper = invocationHelper;
      this.accessGrantedPath = accessGrantedPath;
      this.accessDeniedPath = accessDeniedPath;
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(invocationHelper.getRestCacheManager().getInstance());
      PrincipalRoleMapper mapper = globalConfiguration.security().authorization().roleMapperConfiguration().roleMapper();
      this.principalRoleMapper = mapper != null && mapper instanceof MutablePrincipalRoleMapper ? (MutablePrincipalRoleMapper) mapper : null;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/login").withAction("config")
            .anonymous().handleWith(this::loginConfiguration)
            .invocation().methods(GET, POST).deprecated().path("/v2/login").withAction("login")
            .permission(AuthorizationPermission.NONE).name("USER LOGIN").auditContext(AuditContext.SERVER)
            .handleWith(this::login)
            .invocation().methods(GET).deprecated().path("/v2/login")
            .permission(AuthorizationPermission.NONE).name("USER LOGIN").auditContext(AuditContext.SERVER)
            .handleWith(this::login)
            .invocation().methods(GET).path("/v2/security/user/acl")
            .handleWith(this::acl)
            .invocation().method(GET).path("/v2/security/roles")
            .permission(AuthorizationPermission.ADMIN).name("ROLES").auditContext(AuditContext.SERVER)
            .handleWith(this::listAllRoles)
            .invocation().method(GET).path("/v2/security/roles/{principal}")
            .permission(AuthorizationPermission.ADMIN).name("ROLES PRINCIPAL").auditContext(AuditContext.SERVER)
            .handleWith(this::listPrincipalRoles)
            .invocation().methods(PUT).path("/v2/security/roles/{principal}").withAction("grant")
            .permission(AuthorizationPermission.ADMIN).name("ROLES GRANT").auditContext(AuditContext.SERVER)
            .handleWith(this::grant)
            .invocation().methods(PUT).path("/v2/security/roles/{principal}").withAction("deny")
            .permission(AuthorizationPermission.ADMIN).name("ROLES DENY").auditContext(AuditContext.SERVER)
            .handleWith(this::deny)
            .create();
   }

   private CompletionStage<RestResponse> deny(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      if (principalRoleMapper == null) {
         return completedFuture(new NettyRestResponse.Builder().status(CONFLICT).build());
      }
      String principal = request.variables().get("principal");
      List<String> roles = request.parameters().get("role");
      if (roles == null) {
         return completedFuture(builder.status(HttpResponseStatus.BAD_REQUEST).build());
      }
      roles.forEach(r -> principalRoleMapper.deny(r, principal));
      return completedFuture(builder.status(NO_CONTENT).build());
   }

   private CompletionStage<RestResponse> grant(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      if (principalRoleMapper == null) {
         return completedFuture(new NettyRestResponse.Builder().status(CONFLICT).build());
      }
      String principal = request.variables().get("principal");
      List<String> roles = request.parameters().get("role");
      if (roles == null) {
         return completedFuture(builder.status(HttpResponseStatus.BAD_REQUEST).build());
      }
      roles.forEach(r -> principalRoleMapper.grant(r, principal));
      return completedFuture(builder.status(NO_CONTENT).build());
   }

   private CompletionStage<RestResponse> listAllRoles(RestRequest request) {
      GlobalAuthorizationConfiguration authorization = invocationHelper.getRestCacheManager().getInstance().getCacheManagerConfiguration().security().authorization();
      if (!authorization.enabled()) {
         return completedFuture(new NettyRestResponse.Builder().status(BAD_REQUEST).build());
      }
      Json roles = Json.array();
      authorization.roles().entrySet().stream().filter(e -> e.getValue().isInheritable()).forEach(e -> roles.add(e.getKey()));
      return asJsonResponseFuture(roles);
   }

   private CompletionStage<RestResponse> listPrincipalRoles(RestRequest request) {
      String principal = request.variables().get("principal");
      if (principalRoleMapper == null) {
         return completedFuture(new NettyRestResponse.Builder().status(CONFLICT).build());
      }
      Json roles = Json.array();
      principalRoleMapper.list(principal).forEach(r -> roles.add(r));
      return asJsonResponseFuture(roles);
   }

   private CompletionStage<RestResponse> acl(RestRequest request) {
      Subject subject = request.getSubject();
      RestCacheManager<Object> rcm = invocationHelper.getRestCacheManager();
      Collection<String> cacheNames = rcm.getCacheNames();
      Json acl = Json.object();
      if(subject == null) {
         acl.set("subject", Json.array());
      } else {
         Json jsonSubjects = Json.array();
         subject.getPrincipals().forEach(principal -> {
            jsonSubjects.add(Json.object().set("name", principal.getName()).set("type", principal.getClass().getSimpleName()));
         });
         acl.set("subject", jsonSubjects);
         Authorizer authorizer = rcm.getAuthorizer();
         SubjectACL globalACL = authorizer.getACL(subject);
         acl.set("global", aclToJson(globalACL));
         Json caches = Json.object();
         acl.set("caches", caches);
         for (String cacheName : cacheNames) {
            Configuration cacheConfiguration = SecurityActions.getCacheConfigurationFromManager(rcm.getInstance(), cacheName);
            SubjectACL cacheACL = authorizer.getACL(subject, cacheConfiguration.security().authorization());
            caches.set(cacheName, aclToJson(cacheACL));
         }
      }
      return asJsonResponseFuture(acl);
   }

   private Json aclToJson(SubjectACL acl) {
      Json array = Json.array();
      for (AuthorizationPermission permission : acl.getPermissions()) {
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
