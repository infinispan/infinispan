package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpInputSchema(McpType type, McpProperty... properties) implements JsonSerialization {
   @Override
   public Json toJson() {
      Json required = Json.array();
      Json properties = Json.object();

      for (McpProperty property : this.properties) {
         properties.set(property.name(), property.toJson());
         if (property.required()) {
            required.add(property.name());
         }
      }

      return Json.object()
            .set(McpConstants.TYPE, type.name().toLowerCase())
            .set("properties", properties)
            .set("required", required);
   }
}
