package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
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
 * @since 16.0
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
            .operationId("stopClusterV3")
            .parameter("server", ParameterIn.QUERY, false, Schema.STRING, "Optional server name(s) to stop (can be repeated)")
            .response(NO_CONTENT, "Cluster shutdown initiated")
            .permission(AuthorizationPermission.LIFECYCLE)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::stop);

      builder.invocation()
            .methods(GET).path("/v3/cluster/_distribution")
            .name("Get cluster distribution information")
            .operationId("getClusterDistributionV3")
            .response(OK, "Cluster distribution data", APPLICATION_JSON)
            .permission(AuthorizationPermission.MONITOR)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::distribution);

      // 2. Backup Operations
      builder.invocation()
            .methods(GET, HEAD).path("/v3/cluster/backups")
            .name("List all cluster backup names")
            .operationId("listClusterBackupsV3")
            .response(OK, "List of backup names", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::getAllBackupNames);

      builder.invocation()
            .methods(POST).path("/v3/cluster/backups/{backupName}")
            .name("Create cluster backup")
            .operationId("createClusterBackupV3")
            .parameter("backupName", ParameterIn.PATH, true, Schema.STRING, "Name of the backup")
            .response(CREATED, "Backup created successfully", APPLICATION_JSON)
            .response(OK, "Backup already exists", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      builder.invocation()
            .methods(GET, HEAD).path("/v3/cluster/backups/{backupName}")
            .name("Get cluster backup status")
            .operationId("getClusterBackupStatusV3")
            .parameter("backupName", ParameterIn.PATH, true, Schema.STRING, "Name of the backup")
            .response(OK, "Backup status", APPLICATION_JSON)
            .response(NOT_FOUND, "Backup not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      builder.invocation()
            .methods(DELETE).path("/v3/cluster/backups/{backupName}")
            .name("Delete cluster backup")
            .operationId("deleteClusterBackupV3")
            .parameter("backupName", ParameterIn.PATH, true, Schema.STRING, "Name of the backup")
            .response(NO_CONTENT, "Backup deleted successfully")
            .response(NOT_FOUND, "Backup not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::backup);

      // 3. Restore Operations
      builder.invocation()
            .methods(GET).path("/v3/cluster/restores")
            .name("List all cluster restore names")
            .operationId("listClusterRestoresV3")
            .response(OK, "List of restore names", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::getAllRestoreNames);

      builder.invocation()
            .methods(POST, HEAD).path("/v3/cluster/restores/{restoreName}")
            .name("Create cluster restore")
            .operationId("createClusterRestoreV3")
            .parameter("restoreName", ParameterIn.PATH, true, Schema.STRING, "Name of the restore")
            .response(CREATED, "Restore created successfully", APPLICATION_JSON)
            .response(OK, "Restore already exists", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::restore);

      builder.invocation()
            .methods(DELETE).path("/v3/cluster/restores/{restoreName}")
            .name("Delete cluster restore")
            .operationId("deleteClusterRestoreV3")
            .parameter("restoreName", ParameterIn.PATH, true, Schema.STRING, "Name of the restore")
            .response(NO_CONTENT, "Restore deleted successfully")
            .response(NOT_FOUND, "Restore not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::restore);

      // 4. RAFT Management
      builder.invocation()
            .methods(GET).path("/v3/cluster/raft")
            .name("List RAFT members")
            .operationId("listRaftMembersV3")
            .response(OK, "List of RAFT member IDs", APPLICATION_JSON)
            .response(NO_CONTENT, "RAFT not available")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleRaftMembers);

      builder.invocation()
            .methods(POST, PUT).path("/v3/cluster/raft/{member}")
            .name("Add RAFT member")
            .operationId("addRaftMemberV3")
            .parameter("member", ParameterIn.PATH, true, Schema.STRING, "RAFT member ID to add")
            .response(OK, "RAFT member added successfully")
            .response(NOT_FOUND, "RAFT not available")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleAddRaftMember);

      builder.invocation()
            .methods(DELETE).path("/v3/cluster/raft/{member}")
            .name("Remove RAFT member")
            .operationId("removeRaftMemberV3")
            .parameter("member", ParameterIn.PATH, true, Schema.STRING, "RAFT member ID to remove")
            .response(OK, "RAFT member removed successfully")
            .response(NOT_FOUND, "RAFT not available")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleRemoveRaftMember);

      // 5. Cluster Members
      builder.invocation()
            .methods(GET).path("/v3/cluster/members")
            .name("Get cluster member information")
            .operationId("getClusterMembersV3")
            .response(OK, "Cluster member information including rolling upgrade status", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::handleClusterMembers);

      return builder.create();
   }
}
