package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.HIT;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonRawValue;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Represents each of the search results.
 *
 * @since 9.2
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
