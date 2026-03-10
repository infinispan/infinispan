package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.telemetry.InfinispanTelemetry;

/**
 * REST resource to manage Protobuf schemas (V3 OpenAPI compliant).
 * <p>
 * Extends ProtobufResource to reuse handler methods, only defines v3-specific endpoint paths.
 * <p>
 * Rules for OpenAPI v3 compliance:
 * <ul>
 *    <li>Resources should have unique paths</li>
 *    <li>Actions should be prefixed by _</li>
 * </ul>
 *
 * @since 16.1
 */
public class ProtobufResourceV3 extends ProtobufResource implements ResourceHandler {

   public ProtobufResourceV3(InvocationHelper invocationHelper, InfinispanTelemetry telemetryService) {
      super(invocationHelper, telemetryService);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("protobuf", "REST resource to manage Protobuf schemas.")
            // List schemas
            .invocation().methods(GET).path("/v3/schemas")
               .name("List Protobuf schemas")
               .operationId("listSchemas")
               .parameter("pretty", ParameterIn.QUERY, false, Schema.BOOLEAN, "Pretty print the JSON output")
               .response(OK, "List of Protobuf schemas", APPLICATION_JSON)
               .handleWith(this::getSchemasNames)

            // Meta operations (using /v3/meta/schemas namespace to avoid conflicts with schema names)
            .invocation().methods(GET).path("/v3/meta/schemas/_types")
               .name("Get all known Protobuf types")
               .operationId("getProtobufTypes")
               .parameter("pretty", ParameterIn.QUERY, false, Schema.BOOLEAN, "Pretty print the JSON output")
               .response(OK, "List of known Protobuf types", APPLICATION_JSON)
               .handleWith(this::getTypes)

            // Create schema
            .invocation().methods(POST).path("/v3/schemas/{schemaName}")
               .name("Create Protobuf schema")
               .operationId("createSchema")
               .permission(AuthorizationPermission.CREATE).auditContext(AuditContext.SERVER).name("SCHEMA CREATE")
               .request("Protobuf schema content", true, Map.of(TEXT_PLAIN, Schema.NONE))
               .response(CREATED, "Schema created successfully", APPLICATION_JSON)
               .response(CONFLICT, "Schema already exists", APPLICATION_JSON)
               .response(INTERNAL_SERVER_ERROR, "Error creating schema", TEXT_PLAIN, Schema.STRING)
               .handleWith(r -> createOrReplace(r, true))

            // Update/Replace schema
            .invocation().methods(PUT).path("/v3/schemas/{schemaName}")
               .name("Update or replace Protobuf schema")
               .operationId("updateSchema")
               .permission(AuthorizationPermission.CREATE).auditContext(AuditContext.SERVER).name("SCHEMA CREATE")
               .request("Protobuf schema content", true, Map.of(TEXT_PLAIN, Schema.NONE))
               .response(OK, "Schema updated successfully", APPLICATION_JSON)
               .response(INTERNAL_SERVER_ERROR, "Error updating schema", TEXT_PLAIN, Schema.STRING)
               .handleWith(r -> createOrReplace(r, false))

            // Get schema
            .invocation().methods(GET).path("/v3/schemas/{schemaName}")
               .name("Retrieve Protobuf schema")
               .operationId("getSchema")
               .response(OK, "Protobuf schema content", TEXT_PLAIN)
               .response(NOT_FOUND, "Schema not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getSchema)

            // Get schema with detailed information
            .invocation().methods(GET).path("/v3/schemas/{schemaName}/_detailed")
               .name("Retrieve Protobuf schema with detailed information")
               .operationId("getSchemaDetailed")
               .response(OK, "Protobuf schema with metadata (content, validation errors, and caches)", APPLICATION_JSON)
               .response(NOT_FOUND, "Schema not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getSchemaDetailed)

            // Delete schema
            .invocation().method(DELETE).path("/v3/schemas/{schemaName}")
               .name("Delete Protobuf schema")
               .operationId("deleteSchema")
               .permission(AuthorizationPermission.CREATE).auditContext(AuditContext.SERVER).name("SCHEMA DELETE")
               .response(NO_CONTENT, "Schema deleted successfully")
               .response(NOT_FOUND, "Schema not found")
               .handleWith(this::deleteSchema)

            .create();
   }

   private CompletionStage<RestResponse> getSchema(RestRequest request) {
      return getSchema(request, false);
   }

   private CompletionStage<RestResponse> getSchemaDetailed(RestRequest request) {
      return getSchema(request, true);
   }

}
