package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HIT;

import java.util.Map;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * @since 9.4
 */
public class JsonProjection implements JsonSerialization {

   private final Map<String, Object> value;

   JsonProjection(Map<String, Object> value) {
      this.value = value;
   }

   public Map<String, Object> getValue() {
      return value;
   }

   @Override
   public Json toJson() {
      return Json.object().set(HIT, Json.make(value));
   }
}
