package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;

public class InfinispanJacksonJsonNodeProvider extends JacksonJsonNodeJsonProvider {

   // Default Jackson integration doesn't complain when trying to
   // delete array elements out-of-bound. Overriding this method
   // so to behave as setArrayIndex
   public void removeProperty(Object obj, Object key) {
      if (isMap(obj)) {
         toJsonObject(obj).remove(key.toString());
      } else {
         ArrayNode array = toJsonArray(obj);
         int index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
         if (index >= 0 && index < array.size()) {
            array.remove(index);
         } else {
            throw new IndexOutOfBoundsException("Illegal index " + index + ", array size " + array.size());
         }
      }
   }

   private ArrayNode toJsonArray(Object o) {
      return (ArrayNode) o;
   }

   private ObjectNode toJsonObject(Object o) {
      return (ObjectNode) o;
   }
}
