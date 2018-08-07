package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HIT;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Represents each of the search results.
 *
 * @since 9.4
 */
@JsonPropertyOrder({HIT})
public class Hit {

   private final Object value;

   public Hit(Object value) {
      this.value = value;
   }

   @JsonProperty(HIT)
   @JsonRawValue
   @JsonSerialize(using = HitSerializer.class)
   public Object getValue() {
      return value;
   }

}
