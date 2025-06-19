package org.infinispan.rest.framework.openapi;

import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record RequestBody(String description, boolean required,
                          Map<MediaType, Schema> schemas) implements JsonSerialization {
   @Override
   public Json toJson() {
      Json json = Json.object();
      json.set("description", description);
      json.set("required", required);
      Json content = Json.object();
      for (Map.Entry<MediaType, Schema> entry : schemas.entrySet()) {
         Json type = Json.object();
         content.set(entry.getKey().toString(), type);
         Schema schema = entry.getValue();
         if (schema != null) {
            if (schema.isPrimitive()) {
               type.set("schema", schema);
            } else {
               type.set("schema", Json.object("$ref", "#/components/schemas/" + schema.name()));
            }
         }
      }
      json.set("content", content);
      return json;
   }
}
