package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_GZIP;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.Invocations.Builder;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;

/**
 * ServerResourceV3 - REST v3 API for server management operations.
 *
 * Implements OpenAPI-compliant endpoints following v3 standards:
 * - Action-based operations use path segments (e.g., /_stop, /_heap-dump)
 * - Path parameters auto-extracted from path strings
 * - Consistent with v3 resource-oriented design
 *
 * @since 16.1
 */
public class ServerResourceV3 extends ServerResource {

   public ServerResourceV3(InvocationHelper invocationHelper) {
      super(invocationHelper);
   }

   @Override
   public Invocations getInvocations() {
      Builder builder = new Invocations.Builder("server", "Server management operations");

      // 1. Server Info
      builder.invocation()
            .methods(GET).path("/v3/server")
            .name("Retrieve server information")
            .operationId("getServerInfo")
            .response(OK, "Server information", APPLICATION_JSON)
            .handleWith(this::info);

      // 2. Server Configuration
      builder.invocation()
            .methods(GET).path("/v3/server/config")
            .name("Retrieve server configuration")
            .operationId("getServerConfig")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .parameter("pretty", ParameterIn.QUERY, false, Schema.BOOLEAN, "Pretty print the output")
            .response(OK, "Server configuration as XML", APPLICATION_XML)
            .response(OK, "Server configuration as JSON", APPLICATION_JSON)
            .response(OK, "Server configuration as YAML", APPLICATION_YAML)
            .handleWith(this::config);

      // 3. Environment Properties
      builder.invocation()
            .methods(GET).path("/v3/server/env")
            .name("Retrieve environment properties")
            .operationId("getServerEnv")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Environment properties", APPLICATION_JSON)
            .handleWith(this::env);

      // 4. Memory Info
      builder.invocation()
            .methods(GET).path("/v3/server/memory")
            .name("Retrieve JVM memory information")
            .operationId("getServerMemory")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Memory information", APPLICATION_JSON)
            .handleWith(this::memory);

      // 5. Heap Dump
      builder.invocation()
            .methods(POST).path("/v3/server/memory/_heap-dump")
            .name("Generate heap dump")
            .operationId("heapDump")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .parameter("live", ParameterIn.QUERY, false, Schema.BOOLEAN, "Include only live objects")
            .response(OK, "Heap dump file created", APPLICATION_JSON)
            .handleWith(this::heapDump);

      // 6. Stop Server
      builder.invocation()
            .methods(POST).path("/v3/server/_stop")
            .name("Stop the server")
            .operationId("stopServer")
            .permission(AuthorizationPermission.ADMIN)
            .response(NO_CONTENT, "Server stop initiated")
            .handleWith(this::stop);

      // 7. Overview Report
      builder.invocation()
            .methods(GET).path("/v3/server/overview-report")
            .name("Retrieve cluster overview report")
            .operationId("getOverviewReport")
            .permission(AuthorizationPermission.ADMIN)
            .response(OK, "Cluster overview report", APPLICATION_JSON)
            .handleWith(this::overviewReport);

      // 8. Thread Dump
      builder.invocation()
            .methods(GET).path("/v3/server/threads")
            .name("Retrieve thread dump")
            .operationId("getThreadDump")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Thread dump", TEXT_PLAIN)
            .handleWith(this::threads);

      // 9. Server Report
      builder.invocation()
            .methods(GET).path("/v3/server/report")
            .name("Generate server diagnostic report")
            .operationId("getServerReport")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Server diagnostic report", APPLICATION_GZIP)
            .handleWith(this::report);

      // 10. Node Report
      builder.invocation()
            .methods(GET).path("/v3/server/report/{nodeName}")
            .name("Generate diagnostic report for a specific node")
            .operationId("getNodeReport")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Node diagnostic report", APPLICATION_GZIP)
            .response(NOT_FOUND, "Node not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::nodeReport);

      // 11. List Ignored Caches
      builder.invocation()
            .methods(GET).path("/v3/server/ignored-caches")
            .name("List ignored caches")
            .operationId("listIgnoredCaches")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "List of ignored cache names", APPLICATION_JSON)
            .handleWith(this::listIgnored);

      // 12. Ignore Cache (POST)
      builder.invocation()
            .methods(POST).path("/v3/server/ignored-caches/{cache}")
            .name("Mark a cache as ignored")
            .operationId("ignoreCache")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(NO_CONTENT, "Cache marked as ignored")
            .response(NOT_FOUND, "Cache not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::doIgnoreOp);

      // 13. Unignore Cache (DELETE)
      builder.invocation()
            .methods(DELETE).path("/v3/server/ignored-caches/{cache}")
            .name("Remove a cache from the ignored list")
            .operationId("unignoreCache")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(NO_CONTENT, "Cache removed from ignored list")
            .response(NOT_FOUND, "Cache not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::doIgnoreOp);

      // 14. List Connections
      builder.invocation()
            .methods(GET).path("/v3/server/connections")
            .name("List active client connections")
            .operationId("listConnections")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .parameter("global", ParameterIn.QUERY, false, Schema.BOOLEAN, "List connections cluster-wide")
            .response(OK, "List of active connections", APPLICATION_JSON)
            .handleWith(this::listConnections);

      // 15. List Connectors
      builder.invocation()
            .methods(GET).path("/v3/server/connectors")
            .name("List all protocol connectors")
            .operationId("listConnectors")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "List of connector names", APPLICATION_JSON)
            .handleWith(this::listConnectors);

      // 16. Connector Status
      builder.invocation()
            .methods(GET).path("/v3/server/connectors/{connector}")
            .name("Retrieve connector status and configuration")
            .operationId("getConnectorStatus")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Connector status and configuration", APPLICATION_JSON)
            .response(NOT_FOUND, "Connector not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::connectorStatus);

      // 17. Start Connector
      builder.invocation()
            .methods(POST).path("/v3/server/connectors/{connector}/_start")
            .name("Start a connector")
            .operationId("startConnector")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(NO_CONTENT, "Connector started")
            .response(NOT_FOUND, "Connector not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::connectorStart);

      // 18. Stop Connector
      builder.invocation()
            .methods(POST).path("/v3/server/connectors/{connector}/_stop")
            .name("Stop a connector")
            .operationId("stopConnector")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(NO_CONTENT, "Connector stopped")
            .response(NOT_FOUND, "Connector not found", TEXT_PLAIN, Schema.STRING)
            .response(CONFLICT, "Cannot stop connector handling current request", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::connectorStop);

      // 19. Get IP Filter Rules
      builder.invocation()
            .methods(GET).path("/v3/server/connectors/{connector}/ip-filter")
            .name("Retrieve IP filter rules for a connector")
            .operationId("getConnectorIpFilter")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "IP filter rules", APPLICATION_JSON)
            .response(NOT_FOUND, "Connector not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::connectorIpFilterList);

      // 20. Set IP Filter Rules
      builder.invocation()
            .methods(POST).path("/v3/server/connectors/{connector}/ip-filter")
            .name("Set IP filter rules for a connector")
            .operationId("setConnectorIpFilter")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .request("IP filter rules", true, java.util.Map.of(APPLICATION_JSON, Schema.NONE))
            .response(NO_CONTENT, "IP filter rules set")
            .response(NOT_FOUND, "Connector not found", TEXT_PLAIN, Schema.STRING)
            .response(CONFLICT, "Reject rule matches request address", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::connectorIpFilterSet);

      // 21. Clear IP Filter Rules
      builder.invocation()
            .methods(DELETE).path("/v3/server/connectors/{connector}/ip-filter")
            .name("Clear IP filter rules for a connector")
            .operationId("clearConnectorIpFilter")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(NO_CONTENT, "IP filter rules cleared")
            .response(NOT_FOUND, "Connector not found", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::connectorIpFilterClear);

      // 22. List Data Sources
      builder.invocation()
            .methods(GET).path("/v3/server/datasources")
            .name("List all data sources")
            .operationId("listDataSources")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "List of data source names", APPLICATION_JSON)
            .handleWith(this::dataSourceList);

      // 23. Test Data Source
      builder.invocation()
            .methods(POST).path("/v3/server/datasources/{datasource}/_test")
            .name("Test data source connectivity")
            .operationId("testDataSource")
            .permission(AuthorizationPermission.ADMIN)
            .auditContext(AuditContext.SERVER)
            .response(OK, "Data source connection successful", TEXT_PLAIN, Schema.STRING)
            .response(NOT_FOUND, "Data source not found", TEXT_PLAIN, Schema.STRING)
            .response(SERVICE_UNAVAILABLE, "Data source connection failed", TEXT_PLAIN, Schema.STRING)
            .handleWith(this::dataSourceTest);

      // 24. Cache Configuration Defaults
      builder.invocation()
            .methods(GET).path("/v3/server/caches/defaults")
            .name("Retrieve default cache configuration attributes")
            .operationId("getCacheDefaults")
            .response(OK, "Default cache configuration attributes", APPLICATION_JSON)
            .handleWith(this::getCacheConfigDefaultAttributes);

      return builder.create();
   }
}
