package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_ZIP;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.Invocations.Builder;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;

/**
 * ContainerResourceV3 - REST v3 API for container/cache manager operations.
 * <p>
 * Extends ContainerResource to reuse handler methods, only defines v3-specific endpoint paths.
 * <p>
 * Key changes from v2:
 * - Action-based endpoints use underscore-prefixed path segments
 * - Deprecated /cache-managers/{name} endpoints excluded (only /container endpoints included)
 * - Trailing slashes removed for consistency
 * - OpenAPI 3.0 compliance with unique operationIds and proper response schemas
 * - SSE endpoints included: config changes and lifecycle events streaming
 *
 * @since 16.1
 */
public class ContainerResourceV3 extends ContainerResource {

   public ContainerResourceV3(InvocationHelper invocationHelper) {
      super(invocationHelper);
   }

   @Override
   public Invocations getInvocations() {
      Builder builder = new Invocations.Builder("container", "Container/cache manager operations");

      // ===== HEALTH OPERATIONS =====

      // 1. Get Container Health
      builder.invocation()
            .methods(GET, HEAD).path("/v3/container/health")
            .name("Get container health")
            .operationId("getContainerHealth")
            .response(OK, "Health status", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getHealth);

      // 2. Get Health Status (anonymous)
      builder.invocation()
            .methods(GET, HEAD).path("/v3/container/health/status")
            .name("Get health status")
            .operationId("getContainerHealthStatus")
            .anonymous(true)
            .response(OK, "Health status", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::getHealthStatus);

      // ===== CONFIGURATION OPERATIONS =====

      // 3. Get Container Configuration
      builder.invocation()
            .methods(GET).path("/v3/container/config")
            .name("Get container configuration")
            .operationId("getContainerConfig")
            .response(OK, "Container configuration", APPLICATION_JSON)
            .response(OK, "Container configuration", APPLICATION_XML)
            .response(OK, "Container configuration", APPLICATION_YAML)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getConfig);

      // 4. List All Cache Configurations
      builder.invocation()
            .methods(GET).path("/v3/container/cache-configs")
            .name("List all cache configurations")
            .operationId("getAllCacheConfigs")
            .response(OK, "List of cache configurations", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getAllCachesConfiguration);

      // 5. List Configuration Templates
      builder.invocation()
            .methods(GET).path("/v3/container/cache-configs/templates")
            .name("List configuration templates")
            .operationId("getCacheConfigTemplates")
            .response(OK, "List of configuration templates", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getAllCachesConfigurationTemplates);

      // 6. Convert Cache Config to JSON
      builder.invocation()
            .methods(POST).path("/v3/container/_cache-config-to-json")
            .name("Convert cache configuration to JSON")
            .operationId("convertCacheConfigToJson")
            .response(OK, "Configuration in JSON format", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::convertToJson);

      // ===== CONTAINER INFO =====

      // 7. Get Container Info
      builder.invocation()
            .methods(GET).path("/v3/container")
            .name("Get container information")
            .operationId("getContainerInfo")
            .response(OK, "Container information", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getInfo);

      // ===== LIFECYCLE OPERATIONS =====

      // 8. Shutdown Container
      builder.invocation()
            .methods(POST).path("/v3/container/_shutdown")
            .name("Shutdown container")
            .operationId("shutdownContainer")
            .response(NO_CONTENT, "Container shutdown initiated")
            .permission(AuthorizationPermission.LIFECYCLE)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::shutdown);

      // Stops the cache container.
      builder.invocation()
            .methods(POST).path("/v3/container/_leave")
            .name("Stop container")
            .operationId("leaveContainer")
            .response(NO_CONTENT, "Performed stop container")
            .response(ACCEPTED, "If stop did not complete within the given timeout")
            .parameter("timeout", ParameterIn.QUERY, false, Schema.LONG,
                  "Timeout in milliseconds to wait for cache operations to complete")
            .permission(AuthorizationPermission.LIFECYCLE)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::leave);

      // ===== REBALANCING OPERATIONS =====

      // 9. Enable Rebalancing
      builder.invocation()
            .methods(POST).path("/v3/container/_rebalancing-enable")
            .name("Enable rebalancing")
            .operationId("enableRebalancing")
            .response(NO_CONTENT, "Rebalancing enabled")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(request -> setRebalancing(true, request));

      // 10. Disable Rebalancing
      builder.invocation()
            .methods(POST).path("/v3/container/_rebalancing-disable")
            .name("Disable rebalancing")
            .operationId("disableRebalancing")
            .response(NO_CONTENT, "Rebalancing disabled")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(request -> setRebalancing(false, request));

      // ===== STATISTICS OPERATIONS =====

      // 11. Get Statistics
      builder.invocation()
            .methods(GET).path("/v3/container/_stats")
            .name("Get container statistics")
            .operationId("getContainerStats")
            .response(OK, "Container statistics", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getStats);

      // 12. Reset Statistics
      builder.invocation()
            .methods(POST).path("/v3/container/_stats-reset")
            .name("Reset container statistics")
            .operationId("resetContainerStats")
            .response(NO_CONTENT, "Statistics reset")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::resetStats);

      // ===== BACKUP OPERATIONS =====

      // 13. List All Backups
      builder.invocation()
            .methods(GET).path("/v3/container/backups")
            .name("List all backups")
            .operationId("listBackups")
            .response(OK, "List of backup names", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getAllBackupNames);

      // 14. Create Backup
      builder.invocation()
            .methods(POST).path("/v3/container/backups/{backupName}")
            .name("Create backup")
            .operationId("createBackup")
            .response(ACCEPTED, "Backup creation started")
            .response(CONFLICT, "Backup already exists")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::backup);

      // 15. Get Backup Status and data
      builder.invocation()
            .methods(GET).path("/v3/container/backups/{backupName}")
            .name("Get backup status")
            .operationId("getBackupStatus")
            .response(ACCEPTED, "Backup in progress", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Backup not found", TEXT_PLAIN, Schema.STRING)
            .response(OK, "Backup completed, data returned in the entity", APPLICATION_ZIP)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::backup);

      // 16. Check Backup Exists
      builder.invocation()
            .methods(HEAD).path("/v3/container/backups/{backupName}")
            .name("Check if backup exists")
            .operationId("checkBackupExists")
            .response(ACCEPTED, "Backup in progress")
            .response(NOT_FOUND, "Backup not found")
            .response(OK, "Backup completed")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::backup);

      // 17. Delete Backup
      builder.invocation()
            .methods(DELETE).path("/v3/container/backups/{backupName}")
            .name("Delete backup")
            .operationId("deleteBackup")
            .response(NO_CONTENT, "Backup deleted successfully")
            .response(ACCEPTED, "Backup in progress, will be deleted once completed or failed")
            .response(NOT_FOUND, "Backup not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::backup);

      // ===== RESTORE OPERATIONS =====

      // 18. List All Restores
      builder.invocation()
            .methods(GET).path("/v3/container/restores")
            .name("List all restores")
            .operationId("listRestores")
            .response(OK, "List of restore names", APPLICATION_JSON)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::getAllRestoreNames);

      // 19. Create Restore
      builder.invocation()
            .methods(POST).path("/v3/container/restores/{restoreName}")
            .name("Create restore from backup")
            .operationId("createRestore")
            .response(CONFLICT, "Restore already exists")
            .response(UNSUPPORTED_MEDIA_TYPE, "Unsupported media type")
            .response(ACCEPTED, "Restore started")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::restore);

      // 20. Check Restore Exists
      builder.invocation()
            .methods(HEAD).path("/v3/container/restores/{restoreName}")
            .name("Check if restore exists")
            .operationId("checkRestoreExists")
            .response(ACCEPTED, "Restore in progress")
            .response(CREATED, "Restore completed")
            .response(NOT_FOUND, "Restore not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::restore);

      // 21. Delete Restore
      builder.invocation()
            .methods(DELETE).path("/v3/container/restores/{restoreName}")
            .name("Delete restore")
            .operationId("deleteRestore")
            .response(NO_CONTENT, "Restore metadata deleted successfully")
            .response(ACCEPTED, "Restore in progress. Metadata deleted successfully")
            .response(NOT_FOUND, "Restore not found")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::restore);

      // ===== SSE (SERVER-SENT EVENTS) OPERATIONS =====

      // 22. Listen for Configuration Changes (SSE)
      builder.invocation()
            .methods(GET).path("/v3/container/config/_listen")
            .name("Listen for configuration changes")
            .operationId("listenContainerConfig")
            .parameter("includeCurrentState", ParameterIn.QUERY, false, Schema.BOOLEAN,
                      "Include current state of all caches and templates before streaming changes")
            .response(OK, "Server-Sent Events stream of configuration changes", TEXT_EVENT_STREAM)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::listenConfig);

      // 23. Listen for Configuration and Lifecycle Events (SSE)
      builder.invocation()
            .methods(GET).path("/v3/container/_listen")
            .name("Listen for configuration and lifecycle events")
            .operationId("listenContainerLifecycle")
            .parameter("includeCurrentState", ParameterIn.QUERY, false, Schema.BOOLEAN,
                      "Include current state of all caches and templates before streaming events")
            .response(OK, "Server-Sent Events stream of configuration and lifecycle events", TEXT_EVENT_STREAM)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .handleWith(this::listenLifecycle);

      return builder.create();
   }
}
