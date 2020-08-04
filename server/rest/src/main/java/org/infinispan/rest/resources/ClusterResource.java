package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
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
            .invocation().methods(POST).path("/v2/cluster").withAction("stop").handleWith(this::stop)
            .invocation().methods(GET).path("/v2/cluster/backups").handleWith(this::getAllBackupNames)
            .invocation().methods(DELETE, GET, POST).path("/v2/cluster/backups/{backupName}").handleWith(this::backup)
            .invocation().methods(POST).path("/v2/cluster/backups").withAction("restore").handleWith(this::restore)
            .create();
   }

   private CompletionStage<RestResponse> stop(RestRequest restRequest) {
      List<String> servers = restRequest.parameters().get("server");

      if (servers != null && !servers.isEmpty()) {
         invocationHelper.getServer().serverStop(servers);
      } else {
         invocationHelper.getServer().clusterStop();
      }
      return CompletableFuture.completedFuture(new NettyRestResponse.Builder().status(NO_CONTENT).build());
   }

   private CompletionStage<RestResponse> getAllBackupNames(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      Set<String> names = backupManager.getBackupNames();
      return asJsonResponseFuture(Json.make(names));
   }

   private CompletionStage<RestResponse> backup(RestRequest request) {
      return BackupManagerResource.handleBackupRequest(request, backupManager,
            (name, workingDir, json) -> backupManager.create(name, workingDir));
   }

   private CompletionStage<RestResponse> restore(RestRequest request) {
      return BackupManagerResource.handleRestoreRequest(request, (path, json) -> backupManager.restore(path));
   }
}
