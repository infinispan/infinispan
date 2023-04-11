package org.infinispan.server.core.dataconversion.deserializer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SObject extends SEntity {

   private final Map<String, SEntity> fields = new LinkedHashMap<>();

   SObject(String type) {
      super(type);
   }

   void setField(String name, SEntity value) {
      fields.put(name, value);
   }

   public SEntity getField(String name) {
      return fields.get(name);
   }

   @Override
   public Json json() {
      Stack<SEntity> stack = SEntity.items.get();
      if (stack.contains(this)) {
         return Json.object(); // Return a placeholder to avoid circularity
      } else {
         stack.push(this);
         Json json = Json.object();
         for (Map.Entry<String, SEntity> entry : fields.entrySet()) {
            SEntity v = entry.getValue();
            json.set(entry.getKey(), v == null ? Json.nil() : v.json());
         }
         stack.pop();
         return json;
      }
   }
}
