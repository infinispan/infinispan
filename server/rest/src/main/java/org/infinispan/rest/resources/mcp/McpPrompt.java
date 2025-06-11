package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpPrompt(String name, String description, String title, McpArgument... arguments) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json argumentsArray = Json.array();
      for (McpArgument argument : arguments) {
         argumentsArray.add(argument.toJson());
      }

      return Json.object()
            .set(McpConstants.NAME, name)
            .set(McpConstants.DESCRIPTION, description)
            .set("arguments", argumentsArray);
   }
}
