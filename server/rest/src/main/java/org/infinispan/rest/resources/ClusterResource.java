package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.distribution.NodeDistributionInfo;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.BackupManager;

/**
 * @since 10.0
 */
public class ClusterResource implements ResourceHandler {

   private final InvocationHelper invocationHelper;
   private final BackupManager backupManager;

   public ClusterResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.backupManager = invocationHelper.getServer().getBackupManager();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(POST).path("/v2/cluster").withAction("stop")
               .permission(AuthorizationPermission.LIFECYCLE).name("CLUSTER STOP").auditContext(AuditContext.SERVER)
               .handleWith(this::stop)
            .invocation().method(GET).path("/v2/cluster").withAction("distribution")
               .permission(AuthorizationPermission.MONITOR).name("CLUSTER DISTRIBUTION").auditContext(AuditContext.SERVER)
               .handleWith(this::distribution)
            .invocation().methods(GET, HEAD).path("/v2/cluster/backups")
               .permission(AuthorizationPermission.ADMIN).name("BACKUP NAMES").auditContext(AuditContext.SERVER)
               .handleWith(this::getAllBackupNames)
            .invocation().methods(DELETE, GET, HEAD, POST).path("/v2/cluster/backups/{backupName}")
               .permission(AuthorizationPermission.ADMIN).name("BACKUP").auditContext(AuditContext.SERVER)
               .handleWith(this::backup)
            .invocation().methods(GET).path("/v2/cluster/restores")
               .permission(AuthorizationPermission.ADMIN).name("RESTORE NAMES").auditContext(AuditContext.SERVER)
               .handleWith(this::getAllRestoreNames)
            .invocation().methods(DELETE, HEAD, POST).path("/v2/cluster/restores/{restoreName}")
               .permission(AuthorizationPermission.ADMIN).name("RESTORE").auditContext(AuditContext.SERVER)
               .handleWith(this::restore)
            .create();
   }

   private CompletionStage<RestResponse> stop(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> {
         List<String> servers = request.parameters().get("server");

         if (servers != null && !servers.isEmpty()) {
            Security.doAs(request.getSubject(), () -> invocationHelper.getServer().serverStop(servers));
         } else {
            Security.doAs(request.getSubject(), () -> invocationHelper.getServer().clusterStop());
         }
         return invocationHelper.newResponse(request).status(NO_CONTENT).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getAllBackupNames(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      Set<String> names = Security.doAs(request.getSubject(), backupManager::getBackupNames);
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(names), isPretty(request));
   }

   private CompletionStage<RestResponse> backup(RestRequest request) {
      return BackupManagerResource.handleBackupRequest(invocationHelper, request, backupManager,
            (name, workingDir, json) -> backupManager.create(name, workingDir));
   }

   private CompletionStage<RestResponse> getAllRestoreNames(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      Set<String> names = Security.doAs(request.getSubject(), backupManager::getRestoreNames);
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(names), isPretty(request));
   }

   private CompletionStage<RestResponse> restore(RestRequest request) {
      return BackupManagerResource.handleRestoreRequest(invocationHelper, request, backupManager, (name, path, json) -> backupManager.restore(name, path));
   }

   private CompletionStage<RestResponse> distribution(RestRequest request) {
      boolean pretty = isPretty(request);
      return clusterDistribution()
            .thenApply(distributions -> asJsonResponse(invocationHelper.newResponse(request), Json.array(distributions.stream()
                  .map(NodeDistributionInfo::toJson).toArray()), pretty));
   }

   private CompletionStage<List<NodeDistributionInfo>> clusterDistribution() {
      EmbeddedCacheManager cacheManager = invocationHelper.getProtocolServer().getCacheManager();
      List<Address> members = cacheManager.getMembers();
      if (members == null) {
         NodeDistributionInfo info = NodeDistributionInfo
               .resolve(cacheManager.getCacheManagerInfo(), SecurityActions.getGlobalComponentRegistry(cacheManager));
         return CompletableFuture.completedFuture(Collections.singletonList(info));
      }

      Map<Address, NodeDistributionInfo> distributions = new ConcurrentHashMap<>(members.size());
      return SecurityActions.getClusterExecutor(cacheManager)
            .submitConsumer(
                  ecm -> NodeDistributionInfo.resolve(ecm.getCacheManagerInfo(), SecurityActions.getGlobalComponentRegistry(ecm)),
                  (address, info, t) -> {
                     if (t != null) {
                        throw CompletableFutures.asCompletionException(t);
                     }
                     distributions.putIfAbsent(address, info);
                  })
            .thenApply(ignore -> {
               Collection<NodeDistributionInfo> collection = distributions.values();
               return Immutables.immutableListWrap(collection.toArray(new NodeDistributionInfo[0]));
            });
   }
}
