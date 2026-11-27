package org.infinispan.server.core.dataconversion.deserializer;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SBlockData extends SEntity {

   // only the length is rendered, never the payload, so the bytes are read and discarded as they are counted (see
   // Deserializer.blockData) instead of being buffered
   private final long length;

   SBlockData(long length) {
      super("byte[]");
      this.length = length;
   }

   @Override
   public Json json() {
      return Json.make("byte[" + length + "]");
   }
}
