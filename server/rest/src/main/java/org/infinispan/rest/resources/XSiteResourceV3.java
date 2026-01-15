package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.GET;
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
 * XSiteResourceV3 - REST v3 API for cross-site replication operations.
 * <p>
 * Extends XSiteResource to reuse handler methods, only defines v3-specific endpoint paths.
 * <p>
 * Key changes from v2:
 * - Action-based endpoints use underscore-prefixed path segments
 * - Deprecated /cache-managers/{name} endpoints excluded (only /container endpoints included)
 * - Trailing slashes removed for consistency
 * - OpenAPI 3.0 compliance with unique operationIds and proper response schemas
 * <p>
 * Cross-site operations are divided into two levels:
 * - Cache-level: Operations on specific cache backup sites
 * - Container-level: Global operations across all caches
 *
 * @since 16.1
 */
public class XSiteResourceV3 extends XSiteResource {

   public XSiteResourceV3(InvocationHelper invocationHelper) {
      super(invocationHelper);
   }

   @Override
   public Invocations getInvocations() {
      Builder builder = new Invocations.Builder("x-site", "Cross-site replication operations");

      // ===== CACHE-LEVEL CROSS-SITE OPERATIONS =====

      // 1. Clear Push State Status
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/push-state/_clear")
            .name("Clear push state status for cache")
            .operationId("clearCachePushStateStatus")
            .response(OK, "Push state status cleared")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::clearPushStateStatus);

      // 2. List Backup Sites
      builder.invocation()
            .methods(GET).path("/v3/caches/{cacheName}/x-site/backups")
            .name("List backup sites for cache")
            .operationId("listCacheBackupSites")
            .response(OK, "List of backup site statuses", APPLICATION_JSON)
            .response(NOT_FOUND, "Cache not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::backupStatus);

      // 3. Get Push State Status
      builder.invocation()
            .methods(GET).path("/v3/caches/{cacheName}/x-site/push-state/_status")
            .name("Get push state status for cache")
            .operationId("getCachePushStateStatus")
            .response(OK, "Push state status", APPLICATION_JSON)
            .response(NOT_FOUND, "Cache not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::pushStateStatus);

      // 4. Get Site Status
      builder.invocation()
            .methods(GET).path("/v3/caches/{cacheName}/x-site/backups/{site}")
            .name("Get backup site status for cache")
            .operationId("getCacheBackupSiteStatus")
            .response(OK, "Site status", APPLICATION_JSON)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::siteStatus);

      // 5. Take Site Offline
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/backups/{site}/_take-offline")
            .name("Take backup site offline for cache")
            .operationId("takeCacheSiteOffline")
            .response(OK, "Site taken offline", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::takeSiteOffline);

      // 6. Bring Site Online
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/backups/{site}/_bring-online")
            .name("Bring backup site online for cache")
            .operationId("bringCacheSiteOnline")
            .response(OK, "Site brought online", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::bringSiteOnline);

      // 7. Start Push State
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/backups/{site}/_start-push-state")
            .name("Start state push to backup site for cache")
            .operationId("startCachePushState")
            .response(OK, "State push started", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::startStatePush);

      // 8. Cancel Push State
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/backups/{site}/_cancel-push-state")
            .name("Cancel state push to backup site for cache")
            .operationId("cancelCachePushState")
            .response(OK, "State push cancelled", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::cancelPushState);

      // 9. Cancel Receive State
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/backups/{site}/_cancel-receive-state")
            .name("Cancel state receive from backup site for cache")
            .operationId("cancelCacheReceiveState")
            .response(OK, "State receive cancelled", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::cancelReceiveState);

      // 10. Get Take-Offline Configuration
      builder.invocation()
            .methods(GET).path("/v3/caches/{cacheName}/x-site/backups/{site}/take-offline-config")
            .name("Get take-offline configuration for backup site")
            .operationId("getCacheTakeOfflineConfig")
            .response(OK, "Take-offline configuration", APPLICATION_JSON)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::getXSiteTakeOffline);

      // 11. Update Take-Offline Configuration
      builder.invocation()
            .methods(PUT).path("/v3/caches/{cacheName}/x-site/backups/{site}/take-offline-config")
            .name("Update take-offline configuration for backup site")
            .operationId("updateCacheTakeOfflineConfig")
            .response(OK, "Configuration updated", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::updateTakeOffline);

      // 12. Get State Transfer Mode
      builder.invocation()
            .methods(GET).path("/v3/caches/{cacheName}/x-site/backups/{site}/state-transfer-mode")
            .name("Get state transfer mode for backup site")
            .operationId("getCacheStateTransferMode")
            .response(OK, "State transfer mode", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::getStateTransferMode);

      // 13. Set State Transfer Mode
      builder.invocation()
            .methods(POST).path("/v3/caches/{cacheName}/x-site/backups/{site}/state-transfer-mode/_set")
            .name("Set state transfer mode for backup site")
            .operationId("setCacheStateTransferMode")
            .parameter("mode", ParameterIn.QUERY, true, Schema.STRING, "State transfer mode (MANUAL or AUTO)")
            .response(NO_CONTENT, "Mode updated")
            .response(NOT_FOUND, "Cache or site not found", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHE)
            .handleWith(this::setStateTransferMode);

      // ===== CONTAINER-LEVEL CROSS-SITE OPERATIONS =====

      // 14. Global Backup Status
      builder.invocation()
            .methods(GET).path("/v3/container/x-site/backups")
            .name("Get global backup status for all caches")
            .operationId("getGlobalBackupStatus")
            .response(OK, "Global backup status", APPLICATION_JSON)
            .response(NOT_FOUND, "Cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::globalStatus);

      // 15. Global Site Status
      builder.invocation()
            .methods(GET).path("/v3/container/x-site/backups/{site}")
            .name("Get global status for specific backup site")
            .operationId("getGlobalSiteStatus")
            .response(OK, "Global site status", APPLICATION_JSON)
            .response(NOT_FOUND, "Site not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::globalStatus);

      // 16. Bring All Caches Online
      builder.invocation()
            .methods(POST).path("/v3/container/x-site/backups/{site}/_bring-online")
            .name("Bring backup site online for all caches")
            .operationId("bringAllCachesOnline")
            .response(OK, "All caches brought online", APPLICATION_JSON)
            .response(NOT_FOUND, "Site not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::bringAllOnline);

      // 17. Take All Caches Offline
      builder.invocation()
            .methods(POST).path("/v3/container/x-site/backups/{site}/_take-offline")
            .name("Take backup site offline for all caches")
            .operationId("takeAllCachesOffline")
            .response(OK, "All caches taken offline", APPLICATION_JSON)
            .response(NOT_FOUND, "Site not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::takeAllOffline);

      // 18. Start Push State for All Caches
      builder.invocation()
            .methods(POST).path("/v3/container/x-site/backups/{site}/_start-push-state")
            .name("Start state push to backup site for all caches")
            .operationId("startPushAllCaches")
            .response(OK, "State push started for all caches", APPLICATION_JSON)
            .response(NOT_FOUND, "Site not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::startPushAll);

      // 19. Cancel Push State for All Caches
      builder.invocation()
            .methods(POST).path("/v3/container/x-site/backups/{site}/_cancel-push-state")
            .name("Cancel state push to backup site for all caches")
            .operationId("cancelPushAllCaches")
            .response(OK, "State push cancelled for all caches", APPLICATION_JSON)
            .response(NOT_FOUND, "Site not found or cross-site not configured", TEXT_PLAIN, Schema.STRING)
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.CACHEMANAGER)
            .handleWith(this::cancelPushAll);

      return builder.create();
   }
}
