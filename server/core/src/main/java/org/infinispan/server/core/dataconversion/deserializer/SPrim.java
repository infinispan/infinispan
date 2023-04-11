package org.infinispan.server.core.dataconversion.deserializer;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class SPrim extends SEntity {

   private static final Map<Class<?>, String> PRIMITIVES = new HashMap<>();

   static {
      for (Class<?> c : new Class<?>[]{
            Boolean.class, Byte.class, Character.class, Double.class,
            Float.class, Integer.class, Long.class, Short.class
      }) {
         try {
            Field type = c.getField("TYPE");
            Class<?> prim = (Class<?>) type.get(null);
            PRIMITIVES.put(c, prim.getName());
         } catch (Exception e) {
            throw new AssertionError(e);
         }
      }
   }

   private final Object value;

   /**
    * Create a representation of the given wrapped primitive object.
    *
    * @param x a wrapped primitive object, for example an Integer if the represented primitive is an int.
    */
   SPrim(Object x) {
      super(PRIMITIVES.get(x.getClass()));
      this.value = x;
   }

   /**
    * The value of the primitive object, wrapped in the corresponding wrapper type, for example Integer if the primitive
    * is an int.
    */
   public Object getValue() {
      return value;
   }

   @Override
   public Json json() {
      return Json.make(value);
   }
}
