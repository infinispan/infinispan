package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;

/**
 * REST resource to perform search administration operations in caches (V3 OpenAPI compliant).
 * Rules for OpenAPI v3 compliance:
 * <ul>
 *    <li>Resources should have unique paths</li>
 *    <li>Actions should be prefixed by _</li>
 * </ul>
 *
 * @since 16.0
 */
public class SearchAdminResourceV3 extends SearchAdminResource implements ResourceHandler {

   public SearchAdminResourceV3(InvocationHelper invocationHelper) {
      super(invocationHelper);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("search", "Search Administration API")
            // Index metamodel
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_index-metamodel")
               .operationId("IndexMetamodel")
               .name("Retrieve the metamodel of the index")
               .response(OK, "Index metamodel", APPLICATION_JSON)
               .response(NOT_FOUND, "Cache not found", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Cache is not indexed", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::indexMetamodel)

            // Search statistics
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_search-stats")
               .operationId("SearchStats")
               .name("Retrieve search statistics")
               .parameter("scope", ParameterIn.QUERY, false, Schema.STRING, "Statistics scope: 'local' (default) or 'cluster'")
               .response(OK, "Search statistics", APPLICATION_JSON)
               .response(NOT_FOUND, "Cache not found or statistics not enabled", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Cache is not indexed", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::searchStats)

            .invocation().methods(POST).path("/v3/caches/{cacheName}/_search-stats-clear")
               .operationId("clearSearchStats")
               .name("Clear search statistics")
               .parameter("scope", ParameterIn.QUERY, false, Schema.STRING, "Statistics scope: 'local' (default) or 'cluster'")
               .response(NO_CONTENT, "Search statistics cleared")
               .response(NOT_FOUND, "Cache not found or statistics not enabled", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Cache is not indexed or cluster scope is not supported", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::clearSearchStats)

            // Reindex operations
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_reindex")
               .operationId("reindex")
               .name("Rebuild the index for the cache")
               .parameter("local", ParameterIn.QUERY, false, Schema.BOOLEAN, "Whether to run reindex locally only. Default: false")
               .parameter("mode", ParameterIn.QUERY, false, Schema.STRING, "Execution mode: 'sync' (default) or 'async'")
               .response(NO_CONTENT, "Reindex operation completed or started")
               .response(NOT_FOUND, "Cache not found", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Cache is not indexed", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::reindex)

            .invocation().methods(POST).path("/v3/caches/{cacheName}/_index-clear")
               .operationId("clearIndexes")
               .name("Clear all indexes for the cache")
               .parameter("mode", ParameterIn.QUERY, false, Schema.STRING, "Execution mode: 'sync' (default) or 'async'")
               .response(NO_CONTENT, "Index clear operation completed or started")
               .response(NOT_FOUND, "Cache not found", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Cache is not indexed", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::clearIndexes)

            .invocation().methods(POST).path("/v3/caches/{cacheName}/_index-schema-update")
               .operationId("updateIndexSchema")
               .name("Update the index schema")
               .response(NO_CONTENT, "Index schema updated")
               .response(NOT_FOUND, "Cache not found", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Cache is not indexed", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::updateSchema)

            .create();
   }
}
