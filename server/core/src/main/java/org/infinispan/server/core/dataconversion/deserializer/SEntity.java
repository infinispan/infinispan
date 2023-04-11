package org.infinispan.server.core.dataconversion.deserializer;

import java.util.Stack;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public abstract class SEntity {

   protected static final ThreadLocal<Stack<SEntity>> items = ThreadLocal.withInitial(Stack::new);
   private final String type;

   SEntity(String type) {
      this.type = type;
   }

   public abstract Json json();

   String getType() {
      return type;
   }
}
