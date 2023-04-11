package org.infinispan.server.core.dataconversion.deserializer;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SString extends SEntity {
   private final String s;

   SString(String s) {
      super("String");
      this.s = s;
   }

   public String getValue() {
      return s;
   }

   @Override
   public Json json() {
      return Json.make(s);
   }
}
