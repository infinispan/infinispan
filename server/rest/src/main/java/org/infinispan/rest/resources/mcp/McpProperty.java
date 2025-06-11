package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpProperty(String name, McpType type, String description,
                          boolean required) implements JsonSerialization {

   @Override
   public Json toJson() {
      return Json.object()
            .set(McpConstants.TYPE, type.name().toLowerCase())
            .set(McpConstants.DESCRIPTION, description);
   }
}
