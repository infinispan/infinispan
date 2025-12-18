package org.infinispan.rest.framework.openapi;

import java.util.Collection;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record Paths(Collection<Path> paths) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json json = Json.object();
      for (Path path : paths) {
         if (json.has(path.path())) {
            json.at(path.path()).set(path.method().name().toLowerCase(), path.operation().toJson());
         } else {
            json.set(path.path(), path);
         }
      }
      return json;
   }
}
