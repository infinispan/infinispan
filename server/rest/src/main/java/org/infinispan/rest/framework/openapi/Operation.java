package org.infinispan.rest.framework.openapi;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.rest.framework.ResourceDescription;

public record Operation(
      String operationId, String summary,
      String description,
      boolean deprecated,
      ResourceDescription group,
      List<Parameter> parameters,
      RequestBody requestBody,
      Collection<ResponseContent> responses
) implements JsonSerialization {

   public Operation {
      if (responses == null) {
         throw new RuntimeException("No responses for operation " + summary + " in group " + group);
      }
   }

   @Override
   public Json toJson() {
      Json params = Json.array();
      if (parameters != null) {
         for (Parameter parameter : parameters) {
            params.add(parameter);
         }
      }
      Json responses = Json.object();
      for (ResponseContent response : this.responses) {
         responses.set(String.valueOf(response.status().code()), response);
      }

      return Json.object()
            .setIfNotNull("operationId", operationId)
            .set("summary", summary)
            .set("description", description)
            .set("tags", Collections.singleton(group.group()))
            .set("deprecated", deprecated)
            .set("parameters", params)
            .setIfNotNull("requestBody", requestBody)
            .set("responses", responses);
   }
}
