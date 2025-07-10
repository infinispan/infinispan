package org.infinispan.rest.responses;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record MutableAttribute() implements JsonSerialization {
   @Override
   public Json toJson() {
      return null;
   }
}
