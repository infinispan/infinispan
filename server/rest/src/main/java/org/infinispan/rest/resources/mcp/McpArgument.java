package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpArgument(String name, String description, boolean required) implements JsonSerialization {

   @Override
   public Json toJson() {
      return Json.object()
            .set(McpConstants.NAME, name)
            .set(McpConstants.DESCRIPTION, description)
            .set("required", required);
   }
}
