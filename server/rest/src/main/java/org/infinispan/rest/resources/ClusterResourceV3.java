package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_ZIP;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.Invocations.Builder;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;

/**
 * ClusterResourceV3 - REST v3 API for cluster management operations.
 * <p>
 * Extends ClusterResource to reuse handler methods, only defines v3-specific endpoint paths.
 * <p>
 * Key changes from v2:
 * - Action-based endpoints use underscore-prefixed path segments
 *   - POST /v2/cluster?action=stop → POST /v3/cluster/_stop
 *   - GET /v2/cluster?action=distribution → GET /v3/cluster/_distribution
 * - All other endpoints retain the same structure with /v3/ prefix
 * - OpenAPI 3.0 compliance with unique operationIds and proper response schemas
 *
 * @since 16.1
 */
public class ClusterResourceV3 extends ClusterResource {

   public ClusterResourceV3(InvocationHelper invocationHelper) {
      super(invocationHelper);
   }

   @Override
   public Invocations getInvocations() {
      Builder builder = new Invocations.Builder("cluster", "Cluster management operations");

      // 1. Cluster Control Operations
      builder.invocation()
            .methods(POST).path("/v3/cluster/_stop")
            .name("Stop cluster servers")
            .operationId("stopCluster")
            .parameter("server", ParameterIn.QUERY, false, Schema.STRING, "Optional server name(s) to stop (can be repeated)")
            .response(NO_CONTENT, "Cluster shutdown initiated")
            .permission(AuthorizationPermission.LIFECYCLE)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::stop);

      builder.invocation()
            .methods(GET).path("/v3/cluster/_distribution")
            .name("Get cluster distribution information")
            .operationId("getClusterDistribution")
            .response(OK, "Cluster distribution data", APPLICATION_JSON)
            .permission(AuthorizationPermission.MONITOR)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::distribution);

      // 2. Backup Operations
      builder.invocation()
            .methods(GET, HEAD).path("/v3/cluster/backups")
            .name("List all cluster backup names")
            .operationId("listClusterBackups")
            .response(OK, "List of backup names", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::getAllBackupNames);

      builder.invocation()
            .methods(POST).path("/v3/cluster/backups/{backupName}")
            .name("Create cluster backup")
            .operationId("createClusterBackup")
            .response(CONFLICT, "Backup already exists")
            .response(ACCEPTED, "Backup started")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      builder.invocation()
            .methods(GET).path("/v3/cluster/backups/{backupName}")
            .name("Get cluster backup status")
            .operationId("getClusterBackupStatus")
            .response(OK, "Backup status", APPLICATION_ZIP)
            .response(ACCEPTED, "Backup in progress")
            .response(NOT_FOUND, "Backup not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      builder.invocation()
            .methods(HEAD).path("/v3/cluster/backups/{backupName}")
            .name("Get cluster backup status")
            .operationId("getClusterBackupStatus")
            .response(OK, "Backup status")
            .response(ACCEPTED, "Backup in progress")
            .response(NOT_FOUND, "Backup not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      builder.invocation()
            .methods(DELETE).path("/v3/cluster/backups/{backupName}")
            .name("Delete cluster backup")
            .operationId("deleteClusterBackup")
            .response(NO_CONTENT, "Backup deleted successfully")
            .response(ACCEPTED, "Backup in progress, will be deleted once completed or failed")
            .response(NOT_FOUND, "Backup not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      // 3. Restore Operations
      builder.invocation()
            .methods(GET).path("/v3/cluster/restores")
            .name("List all cluster restore names")
            .operationId("listClusterRestores")
            .response(OK, "List of restore names", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::getAllRestoreNames);

      builder.invocation()
            .methods(HEAD).path("/v3/cluster/restores/{restoreName}")
            .name("Check cluster restore status")
            .operationId("checkClusterRestoreStatus")
            .response(ACCEPTED, "Restore in progress")
            .response(CREATED, "Restore completed")
            .response(NOT_FOUND, "Restore not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::restore);

      builder.invocation()
            .methods(POST).path("/v3/cluster/restores/{restoreName}")
            .name("Create cluster restore")
            .operationId("createClusterRestore")
            .response(CONFLICT, "Restore already exists")
            .response(UNSUPPORTED_MEDIA_TYPE, "Unsupported media type")
            .response(ACCEPTED, "Restore started")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::restore);

      builder.invocation()
            .methods(DELETE).path("/v3/cluster/restores/{restoreName}")
            .name("Delete cluster restore metadata")
            .operationId("deleteClusterRestore")
            .response(NO_CONTENT, "Restore metadata deleted successfully")
            .response(ACCEPTED, "Restore in progress. Metadata deleted successfully")
            .response(NOT_FOUND, "Restore not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::restore);

      // 4. RAFT Management
      builder.invocation()
            .methods(GET).path("/v3/cluster/raft")
            .name("List RAFT members")
            .operationId("listRaftMembers")
            .response(OK, "List of RAFT member IDs", APPLICATION_JSON)
            .response(NO_CONTENT, "RAFT not available")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleRaftMembers);

      builder.invocation()
            .methods(POST, PUT).path("/v3/cluster/raft/{member}")
            .name("Add RAFT member")
            .operationId("addRaftMember")
            .response(OK, "RAFT member added successfully")
            .response(NOT_FOUND, "RAFT not available")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleAddRaftMember);

      builder.invocation()
            .methods(DELETE).path("/v3/cluster/raft/{member}")
            .name("Remove RAFT member")
            .operationId("removeRaftMember")
            .response(OK, "RAFT member removed successfully")
            .response(NOT_FOUND, "RAFT not available")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleRemoveRaftMember);

      // 5. Cluster Members
      builder.invocation()
            .methods(GET).path("/v3/cluster/members")
            .name("Get cluster member information")
            .operationId("getClusterMembers")
            .response(OK, "Cluster member information including rolling upgrade status", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleClusterMembers);

      return builder.create();
   }
}
