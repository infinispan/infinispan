package org.infinispan.rest.framework.openapi;

import java.lang.reflect.Field;
import java.util.EnumSet;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record Schema(Class<?> clazz) implements JsonSerialization {
   public static final Schema BOOLEAN = new Schema(boolean.class);
   public static final Schema INTEGER = new Schema(int.class);
   public static final Schema STRING = new Schema(String.class);

   @Override
   public Json toJson() {
      return inspect(clazz, true);
   }

   private static Json inspect(Class<?> clazz, boolean named) {
      boolean done = true;
      Json json = Json.object();
      if (clazz == boolean.class || clazz == Boolean.class) {
         json.set("type", "boolean");
      } else if (clazz == int.class || clazz == Integer.class) {
         json.set("type", "integer");
         json.set("format", "int32");
      } else if (clazz == long.class || clazz == Long.class) {
         json.set("type", "integer");
         json.set("format", "int64");
      } else if (clazz == float.class) {
         json.set("type", "number");
         json.set("format", "float");
      } else if (clazz == double.class) {
         json.set("type", "number");
         json.set("format", "float");
      } else if (clazz == short.class || clazz == Short.class) {
         json.set("type", "number");
         json.set("minimum", Short.MIN_VALUE);
         json.set("maximum", Short.MAX_VALUE);
      } else  if (clazz == byte.class || clazz == Byte.class) {
         json.set("type", "number");
         json.set("minimum", Byte.MIN_VALUE);
         json.set("maximum", Byte.MAX_VALUE);
      } else if (clazz == String.class) {
         json.set("type", "string");
      } else if (clazz.isEnum()) {
         json.set("type", "string");
         json.set("enum", Json.array(EnumSet.allOf((Class<Enum>) clazz).toArray()));
      } else if (clazz.isArray()) {
         json.set("type", "array");
         Json items = inspect(clazz.componentType(), false);
         json.set("items", items);
      } else {
         if ((done = !isLocalClass(clazz))) {
            json.set("type", "object");
         }
      }
      if (done) return json;

      Json properties = Json.object();
      for (Field field : clazz.getDeclaredFields()) {
         Json f = inspect(field.getType(), true);
         properties.set(field.getName(), f);
      }

      Json internal = Json.object()
            .set("type", "object")
            .set("properties", properties);
      json.set(clazz.getSimpleName(), internal);
      return json;
   }

   private static boolean isLocalClass(Class<?> clazz) {
      return clazz.getPackage().getName().startsWith("org.infinispan");
   }
}
