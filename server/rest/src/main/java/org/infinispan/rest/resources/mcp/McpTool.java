package org.infinispan.rest.resources.mcp;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.rest.framework.RestRequest;

public record McpTool(String name, String description, McpInputSchema inputSchema,
                      BiFunction<RestRequest, Json, CompletionStage<Json>> callback) implements JsonSerialization {

   @Override
   public Json toJson() {
      return Json.object()
            .set(McpConstants.NAME, name)
            .set(McpConstants.DESCRIPTION, description)
            .set(McpConstants.INPUT_SCHEMA, inputSchema.toJson());
   }
}
