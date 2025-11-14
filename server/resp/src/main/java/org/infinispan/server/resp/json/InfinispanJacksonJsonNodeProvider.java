package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;

public class InfinispanJacksonJsonNodeProvider extends JacksonJsonNodeJsonProvider {

   public InfinispanJacksonJsonNodeProvider() {
      super();
   }

   public InfinispanJacksonJsonNodeProvider(ObjectMapper objectMapper) {
      super(objectMapper);
   }

   // Default Jackson integration doesn't complain when trying to
   // delete array elements out-of-bound. Overriding this method
   // so to behave as setArrayIndex
   public void removeProperty(Object obj, Object key) {
      if (isMap(obj)) {
         toJsonObject(obj).remove(key.toString());
      } else {
         ArrayNode array = toJsonArray(obj);
         int index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
         int effectiveIndex = index >= 0 ? index : array.size() + index;
         if (effectiveIndex >= 0 && effectiveIndex < array.size()) {
            array.remove(effectiveIndex);
         } else {
            throw new IndexOutOfBoundsException("Illegal index " + index + ", array size " + array.size());
         }
      }
   }

   // Default Jackson integration doesn't handle negative indexes when trying to
   // set array elements. Overriding this method
   @Override
   public void setArrayIndex(Object array, int index, Object newValue) {
      if (!this.isArray(array)) {
         throw new UnsupportedOperationException();
      } else {
         ArrayNode arrayNode = this.toJsonArray(array);
         int effectiveIndex = index >= 0 ? index : arrayNode.size() + index;
         super.setArrayIndex(array, effectiveIndex, newValue);
      }
   }

   private ArrayNode toJsonArray(Object o) {
      return (ArrayNode) o;
   }

   private ObjectNode toJsonObject(Object o) {
      return (ObjectNode) o;
   }
}
