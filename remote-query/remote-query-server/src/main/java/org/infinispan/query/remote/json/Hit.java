package org.infinispan.query.remote.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.query.remote.json.JSONConstants.HIT;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Represents each of the search results.
 *
 * @since 9.4
 */
public class Hit implements JsonSerialization {

   private final Object value;

   public Hit(Object value) {
      this.value = value;
   }

   public Object getValue() {
      return value;
   }

   @Override
   public Json toJson() {
      String rawJson = value instanceof String ? value.toString() : new String((byte[]) value, UTF_8);
      return Json.object().set(HIT, Json.factory().raw(rawJson));
   }

}
