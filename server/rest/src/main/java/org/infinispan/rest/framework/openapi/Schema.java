package org.infinispan.rest.framework.openapi;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.EnumSet;

import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record Schema(Class<?> clazz) implements JsonSerialization {
   public static final Schema BOOLEAN = new Schema(boolean.class);
   public static final Schema INTEGER = new Schema(int.class);
   public static final Schema LONG = new Schema(long.class);
   public static final Schema STRING = new Schema(String.class);
   public static final Schema NONE = new Schema(Void.class);
   public static final Schema STRING_ARRAY = new Schema(String[].class);

   public String name() {
      return clazz.getSimpleName();
   }

   public boolean isPrimitive() {
      return clazz.getPackageName().startsWith("java.lang") || clazz.isEnum();
   }

   @Override
   public Json toJson() {
      Json json = Json.object();
      inspect(clazz, json);
      return json;
   }

   private static void inspect(Class<?> clazz, Json json) {
      if (clazz == boolean.class || clazz == Boolean.class) {
         json.set("type", "boolean");
         return;
      } else if (clazz == int.class || clazz == Integer.class) {
         json.set("type", "integer");
         json.set("format", "int32");
         return;
      } else if (clazz == long.class || clazz == Long.class) {
         json.set("type", "integer");
         json.set("format", "int64");
         return;
      } else if (clazz == float.class) {
         json.set("type", "number");
         json.set("format", "float");
         return;
      } else if (clazz == double.class) {
         json.set("type", "number");
         json.set("format", "float");
         return;
      } else if (clazz == short.class || clazz == Short.class) {
         json.set("type", "number");
         json.set("minimum", Short.MIN_VALUE);
         json.set("maximum", Short.MAX_VALUE);
         return;
      } else  if (clazz == byte.class || clazz == Byte.class) {
         json.set("type", "number");
         json.set("minimum", Byte.MIN_VALUE);
         json.set("maximum", Byte.MAX_VALUE);
         return;
      } else if (clazz == String.class) {
         json.set("type", "string");
         return;
      } else if (clazz.isEnum()) {
         json.set("type", "string");
         json.set("enum", Json.array(EnumSet.allOf((Class<Enum>) clazz).toArray()));
         return;
      } else if (clazz.isArray()) {
         json.set("type", "array");
         Json items = Json.object();
         inspect(clazz.componentType(), items);
         json.set("items", items);
         return;
      }
      json.set("type", "object");
      Json properties = Json.object();
      for (Field field : clazz.getDeclaredFields()) {
         if ((field.getModifiers() & Modifier.STATIC) == 0) {
            Json f = Json.object();
            inspect(field.getType(), f);
            properties.set(NamingStrategy.SNAKE_CASE.convert(field.getName()), f);
         }
      }
      json.set("properties", properties);
   }
}
