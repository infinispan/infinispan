package org.infinispan.server.core.dataconversion.deserializer;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SBlockData extends SEntity {
   private final byte[] data;

   SBlockData(byte[] data) {
      super("byte[]");
      this.data = data;
   }

   @Override
   public Json json() {
      return Json.make("byte[" + data.length + "]");
   }
}
