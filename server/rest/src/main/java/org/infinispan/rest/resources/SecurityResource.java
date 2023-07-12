package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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
import org.infinispan.rest.logging.Log;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.MutablePrincipalRoleMapper;
import org.infinispan.security.MutableRolePermissionMapper;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.security.impl.CacheRoleImpl;
import org.infinispan.security.impl.SubjectACL;
import org.infinispan.server.core.ServerManagement;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.1
 */
public class SecurityResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private final String accessGrantedPath;
   private final MutablePrincipalRoleMapper principalRoleMapper;
   private final MutableRolePermissionMapper rolePermissionMapper;

   public SecurityResource(InvocationHelper invocationHelper, String accessGrantedPath, String accessDeniedPath) {
      this.invocationHelper = invocationHelper;
      this.accessGrantedPath = accessGrantedPath;
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(invocationHelper.getRestCacheManager().getInstance());
      PrincipalRoleMapper mapper = globalConfiguration.security().authorization().principalRoleMapper();
      this.principalRoleMapper = mapper instanceof MutablePrincipalRoleMapper ? (MutablePrincipalRoleMapper) mapper : null;
      RolePermissionMapper permissionMapper = globalConfiguration.security().authorization().rolePermissionMapper();
      this.rolePermissionMapper = permissionMapper instanceof MutableRolePermissionMapper ? (MutableRolePermissionMapper) permissionMapper : null;
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
            .invocation().method(PUT).path("/v2/security/roles/{principal}").withAction("grant")
               .permission(AuthorizationPermission.ADMIN).name("ROLES GRANT").auditContext(AuditContext.SERVER)
               .handleWith(this::grant)
            .invocation().method(PUT).path("/v2/security/roles/{principal}").withAction("deny")
               .permission(AuthorizationPermission.ADMIN).name("ROLES DENY").auditContext(AuditContext.SERVER)
               .handleWith(this::deny)
            .invocation().methods(GET).path("/v2/security/principals")
               .permission(AuthorizationPermission.ADMIN).name("PRINCIPALS LIST").auditContext(AuditContext.SERVER)
               .handleWith(this::listPrincipals)
            .invocation().methods(POST, PUT).path("/v2/security/permissions/{role}")
               .permission(AuthorizationPermission.ADMIN).name("ROLES CREATE").auditContext(AuditContext.SERVER)
               .handleWith(this::createRole)
            .invocation().methods(GET).path("/v2/security/permissions/{role}")
               .permission(AuthorizationPermission.ADMIN).name("ROLES DESCRIBE").auditContext(AuditContext.SERVER)
               .handleWith(this::describeRole)
            .invocation().method(DELETE).path("/v2/security/permissions/{role}")
               .permission(AuthorizationPermission.ADMIN).name("ROLES DELETE").auditContext(AuditContext.SERVER)
               .handleWith(this::deleteRole)
            .invocation().method(POST).path("/v2/security/cache").withAction("flush")
               .permission(AuthorizationPermission.ADMIN).name("ACL CACHE FLUSH").auditContext(AuditContext.SERVER)
               .handleWith(this::aclCacheFlush)
            .create();
   }

   private CompletionStage<RestResponse> listPrincipals(RestRequest request) {
      Json principals = Json.make(invocationHelper.getServer().getPrincipalList());
      return asJsonResponseFuture(invocationHelper.newResponse(request), principals, isPretty(request));
   }

   private CompletionStage<RestResponse> createRole(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      if (rolePermissionMapper == null) {
         return completedFuture(invocationHelper.newResponse(request).status(CONFLICT).entity(Log.REST.rolePermissionMapperNotMutable()).build());
      }
      String name = request.variables().get("role");
      List<String> perms = request.parameters().get("permission");
      if (perms == null) {
         return completedFuture(builder.status(HttpResponseStatus.BAD_REQUEST).build());
      }
      Set<AuthorizationPermission> permissions = perms.stream().map(p -> AuthorizationPermission.valueOf(p.toUpperCase())).collect(Collectors.toSet());
      Role role = new CacheRoleImpl(name, true, permissions);
      return rolePermissionMapper.addRole(role).thenCompose(ignore -> aclCacheFlush(request));
   }

   private CompletionStage<RestResponse> describeRole(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      if (rolePermissionMapper == null) {
         return completedFuture(invocationHelper.newResponse(request).status(CONFLICT).entity(Log.REST.rolePermissionMapperNotMutable()).build());
      }
      String name = request.variables().get("role");

      GlobalAuthorizationConfiguration authorization = invocationHelper.getRestCacheManager().getInstance().getCacheManagerConfiguration().security().authorization();
      Role role = authorization.getRole(name);

      if (role == null) {
         return completedFuture(builder.status(NOT_FOUND).build());
      }
      Json roles = Json.object();
      roles.set("name", name);
      Json permissions = Json.array();
      for (AuthorizationPermission p : role.getPermissions()) {
         permissions.add(p.toString());
      }
      roles.set("permissions", permissions);
      return asJsonResponseFuture(invocationHelper.newResponse(request), roles, isPretty(request));
   }

   private CompletionStage<RestResponse> deleteRole(RestRequest request) {
      if (rolePermissionMapper == null) {
         return completedFuture(invocationHelper.newResponse(request).status(CONFLICT).entity(Log.REST.rolePermissionMapperNotMutable()).build());
      }
      String role = request.variables().get("role");

      return rolePermissionMapper.removeRole(role).thenCompose(ignore -> aclCacheFlush(request));
   }

   private CompletionStage<RestResponse> aclCacheFlush(RestRequest request) {
      ServerManagement server = invocationHelper.getServer();
      return server.flushSecurityCaches().thenApply(v -> invocationHelper.newResponse(request).status(NO_CONTENT).build());
   }

   private CompletionStage<RestResponse> deny(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      if (principalRoleMapper == null) {
         return completedFuture(invocationHelper.newResponse(request).status(CONFLICT).entity(Log.REST.principalRoleMapperNotMutable()).build());
      }
      String principal = request.variables().get("principal");
      List<String> roles = request.parameters().get("role");
      if (roles == null) {
         return completedFuture(builder.status(HttpResponseStatus.BAD_REQUEST).build());
      }
      roles.forEach(r -> principalRoleMapper.deny(r, principal));
      return aclCacheFlush(request);
   }

   private CompletionStage<RestResponse> grant(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      if (principalRoleMapper == null) {
         return completedFuture(invocationHelper.newResponse(request).status(CONFLICT).entity(Log.REST.principalRoleMapperNotMutable()).build());
      }
      String principal = request.variables().get("principal");
      List<String> roles = request.parameters().get("role");
      if (roles == null) {
         return completedFuture(builder.status(HttpResponseStatus.BAD_REQUEST).build());
      }
      roles.forEach(r -> principalRoleMapper.grant(r, principal));
      return aclCacheFlush(request);
   }

   private CompletionStage<RestResponse> listAllRoles(RestRequest request) {
      GlobalAuthorizationConfiguration authorization = invocationHelper.getRestCacheManager().getInstance().getCacheManagerConfiguration().security().authorization();
      if (!authorization.enabled()) {
         throw Log.REST.authorizationNotEnabled();
      }
      Json roles = Json.array();
      authorization.roles().entrySet().stream().filter(e -> e.getValue().isInheritable()).forEach(e -> roles.add(e.getKey()));
      return asJsonResponseFuture(invocationHelper.newResponse(request), roles, isPretty(request));
   }

   private CompletionStage<RestResponse> listPrincipalRoles(RestRequest request) {
      String principal = request.variables().get("principal");
      if (principalRoleMapper == null) {
         return completedFuture(invocationHelper.newResponse(request).status(CONFLICT).entity(Log.REST.principalRoleMapperNotMutable()).build());
      }
      Json roles = Json.array();
      principalRoleMapper.list(principal).forEach(roles::add);
      return asJsonResponseFuture(invocationHelper.newResponse(request), roles, isPretty(request));
   }

   private CompletionStage<RestResponse> acl(RestRequest request) {
      Subject subject = request.getSubject();
      RestCacheManager<Object> rcm = invocationHelper.getRestCacheManager();
      Collection<String> cacheNames = rcm.getCacheNames();
      Json acl = Json.object();
      if (subject == null) {
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
            Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(rcm.getInstance(), cacheName);
            SubjectACL cacheACL = authorizer.getACL(subject, cacheConfiguration.security().authorization());
            caches.set(cacheName, aclToJson(cacheACL));
         }
      }
      return asJsonResponseFuture(invocationHelper.newResponse(request), acl, isPretty(request));
   }

   private Json aclToJson(SubjectACL acl) {
      Json array = Json.array();
      for (AuthorizationPermission permission : acl.getPermissions()) {
         array.add(permission.name());
      }
      return array;
   }

   private CompletionStage<RestResponse> loginConfiguration(RestRequest request) {
      Map<String, String> loginConfiguration = invocationHelper.getServer().getLoginConfiguration(invocationHelper.getProtocolServer());
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(loginConfiguration), isPretty(request));
   }

   private CompletionStage<RestResponse> login(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      responseBuilder.status(HttpResponseStatus.TEMPORARY_REDIRECT).header("Location", accessGrantedPath);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }
}
