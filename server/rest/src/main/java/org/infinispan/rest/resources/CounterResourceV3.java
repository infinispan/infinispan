package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import java.util.Map;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;

/**
 * REST resource to manage counters (v3 OpenAPI-compliant version).
 * <p>
 * Extends CounterResource to reuse handler methods, only defines v3-specific endpoint paths.
 * <p>
 * Rules for OpenAPI v3 compliance:
 * <ul>
 *    <li>Resources should have unique paths</li>
 *    <li>Actions should be prefixed by _</li>
 * </ul>
 *
 * @since 16.1
 */
public class CounterResourceV3 extends CounterResource {

   public CounterResourceV3(InvocationHelper invocationHelper) {
      super(invocationHelper);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("counter", "Counter operations")
            // Lifecycle operations
            .invocation().methods(POST).path("/v3/counters/{counterName}")
               .name("Create counter")
               .operationId("createCounter")
               .request("Counter configuration", true, Map.of(APPLICATION_JSON, Schema.NONE))
               .response(OK, "Counter created successfully")
               .response(NOT_MODIFIED, "Counter already exists or could not be created", TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, "Invalid counter configuration", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::createCounter)

            .invocation().methods(DELETE).path("/v3/counters/{counterName}")
               .name("Delete counter")
               .operationId("deleteCounter")
               .response(NO_CONTENT, "Counter deleted successfully")
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::deleteCounter)

            // Configuration
            .invocation().methods(GET).path("/v3/counters/{counterName}/config")
               .name("Get counter configuration")
               .operationId("getCounterConfig")
               .parameter("pretty", ParameterIn.QUERY, false, Schema.BOOLEAN, "Pretty print the JSON output")
               .response(OK, "Counter configuration", APPLICATION_JSON)
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getConfig)

            // Operations
            .invocation().methods(GET).path("/v3/counters")
               .name("Get all counter names")
               .operationId("getCounterNames")
               .response(OK, "List of counter names", APPLICATION_JSON)
               .handleWith(this::getCounterNames)

            .invocation().methods(GET).path("/v3/counters/{counterName}")
               .name("Get counter value")
               .operationId("getCounterValue")
               .response(OK, "Counter value", TEXT_PLAIN)
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCounter)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_reset")
               .name("Reset counter to initial value")
               .operationId("resetCounter")
               .response(NO_CONTENT, "Counter reset successfully")
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::resetCounter)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_increment")
               .name("Increment counter")
               .operationId("incrementCounter")
               .response(NO_CONTENT, "Weak counter incremented successfully")
               .response(OK, "Strong counter incremented, returns new value", APPLICATION_JSON)
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::incrementCounter)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_decrement")
               .name("Decrement counter")
               .operationId("decrementCounter")
               .response(NO_CONTENT, "Weak counter decremented successfully")
               .response(OK, "Strong counter decremented, returns new value", APPLICATION_JSON)
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::decrementCounter)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_add")
               .name("Add delta to counter")
               .operationId("addDelta")
               .parameter("delta", ParameterIn.QUERY, true, Schema.LONG, "The value to add to the counter (can be negative)")
               .response(NO_CONTENT, "Weak counter updated successfully")
               .response(OK, "Strong counter updated, returns new value", APPLICATION_JSON)
               .response(BAD_REQUEST, "Missing or invalid delta parameter", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, "Counter not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::addValue)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_compare-and-set")
               .name("Compare and set counter value (strong counters only)")
               .operationId("compareAndSet")
               .parameter("expect", ParameterIn.QUERY, true, Schema.LONG, "The expected current value")
               .parameter("update", ParameterIn.QUERY, true, Schema.LONG, "The new value to set if expect matches")
               .response(OK, "Returns true if successful, false otherwise", APPLICATION_JSON)
               .response(BAD_REQUEST, "Missing or invalid parameters", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, "Counter not found or not a strong counter", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::compareSet)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_compare-and-swap")
               .name("Compare and swap counter value (strong counters only)")
               .operationId("compareAndSwap")
               .parameter("expect", ParameterIn.QUERY, true, Schema.LONG, "The expected current value")
               .parameter("update", ParameterIn.QUERY, true, Schema.LONG, "The new value to set if expect matches")
               .response(OK, "Returns the previous value", APPLICATION_JSON, Schema.LONG)
               .response(BAD_REQUEST, "Missing or invalid parameters", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, "Counter not found or not a strong counter", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::compareSwap)

            .invocation().methods(POST).path("/v3/counters/{counterName}/_get-and-set")
               .name("Get and set counter value (strong counters only)")
               .operationId("getAndSet")
               .parameter("value", ParameterIn.QUERY, true, Schema.LONG, "The new value to set")
               .response(OK, "Returns the previous value", APPLICATION_JSON, Schema.LONG)
               .response(BAD_REQUEST, "Missing or invalid value parameter", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, "Counter not found or not a strong counter", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getAndSet)

            .create();
   }
}
