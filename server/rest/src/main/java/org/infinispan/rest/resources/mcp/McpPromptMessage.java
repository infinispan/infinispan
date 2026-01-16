package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpPromptMessage(String role, String text) implements JsonSerialization {

   @Override
   public Json toJson() {
      return Json.object()
            .set("role", role)
            .set("content", Json.object()
                  .set(McpConstants.TYPE, "text")
                  .set("text", text));
   }
}
