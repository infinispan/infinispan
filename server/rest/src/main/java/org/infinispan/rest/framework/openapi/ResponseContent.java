package org.infinispan.rest.framework.openapi;

import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

import io.netty.handler.codec.http.HttpResponseStatus;

public record ResponseContent(
      String description,
      HttpResponseStatus status,
      Map<MediaType, Schema> responses
) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json content = Json.object();
      for (Map.Entry<MediaType, Schema> entry : responses.entrySet()) {
         String name = entry.getKey().toString();
         Schema schema = entry.getValue();
         Json json = Json.object();
         if (schema != null) {
            if (schema.isPrimitive()) {
               json.set("schema", schema);
            } else {
               json.set("schema", Json.object("$ref", "#/components/schemas/" + schema.name()));
            }
         }
         content.set(name, json);
      }
      return Json.object()
            .set("description", description)
            .set("content", content);
   }
}
