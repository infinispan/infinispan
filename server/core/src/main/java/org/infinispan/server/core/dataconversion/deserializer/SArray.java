package org.infinispan.server.core.dataconversion.deserializer;

import java.util.Arrays;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SArray extends SEntity {

   // the backing store starts small and grows on demand, capped at the declared size: the size read from the stream is
   // untrusted and can overshoot the payload actually present, so the full array must not be allocated before its
   // elements are confirmed to exist (see Deserializer.newArray)
   private static final int INITIAL_CAPACITY = 16;

   private final int size;
   private SEntity[] array;

   SArray(String type, int size) {
      super(type);
      this.size = size;
      this.array = new SEntity[Math.min(size, INITIAL_CAPACITY)];
   }

   void set(int i, SEntity object) {
      if (i >= array.length) {
         // grow geometrically but never past the declared size: a completed parse leaves the backing store exactly
         // size long, and a truncated one abandons it having allocated no more than the elements actually read
         long grown = Math.min((long) size, Math.max(array.length * 2L, i + 1L));
         array = Arrays.copyOf(array, (int) grown);
      }
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
