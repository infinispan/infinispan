package org.infinispan.server.core.query.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.server.core.query.json.JSONConstants.HIT;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * Represents each of the search results.
 *
 * @since 9.4
 */
public class Hit implements JsonSerialization {

   protected final Object value;

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
