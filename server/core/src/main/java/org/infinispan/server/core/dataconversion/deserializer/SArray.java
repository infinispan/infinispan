package org.infinispan.server.core.dataconversion.deserializer;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SArray extends SEntity {

   private final SEntity[] array;

   SArray(String type, int size) {
      super(type);
      this.array = new SEntity[size];
   }

   void set(int i, SEntity object) {
      array[i] = object;
   }

   @Override
   public Json json() {
      Json json = Json.array();
      for (SEntity obj : array) {
         json.add(obj.json());
      }
      return json;
   }
}
