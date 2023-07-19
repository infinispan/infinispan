package org.infinispan.server.insights.helper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.infinispan.commons.dataconversion.internal.Json;

import com.fasterxml.jackson.core.JsonGenerator;

public final class JsonGeneratorHelper {

   private JsonGeneratorHelper() {
   }

   public static void write(Json json, JsonGenerator generator) throws IOException {
      if (json.isObject()) {
         writeObject(json, generator);
      } else if (json.isArray()) {
         writeArray(json, generator);
      } else if (json.isString()) {
         generator.writeString(json.asString());
      } else if (json.isBoolean()) {
         generator.writeBoolean(json.asBoolean());
      } else if (json.isNumber()) {
         writeNumber(json.getValue(), generator);
      } else if (json.isNull()) {
         generator.writeNull();
      }
   }

   public static void writeObject(Json json, JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      for (Map.Entry<String, Json> entry : json.asJsonMap().entrySet()) {
         generator.writeFieldName(entry.getKey());
         write(entry.getValue(), generator);
      }
      generator.writeEndObject();
   }

   public static void writeArray(Json json, JsonGenerator generator) throws IOException {
      generator.writeStartArray();
      for (Json value : json.asJsonList()) {
         write(value, generator);
      }
      generator.writeEndArray();
   }

   private static void writeNumber(Object value, JsonGenerator generator) throws IOException {
      if (value instanceof Integer) {
         generator.writeNumber((Integer) value);
      } else if (value instanceof Short) {
         generator.writeNumber((Short) value);
      } else if (value instanceof Long) {
         generator.writeNumber((Long) value);
      } else if (value instanceof BigInteger) {
         generator.writeNumber((BigInteger) value);
      } else if (value instanceof Double) {
         generator.writeNumber((Double) value);
      } else if (value instanceof Float) {
         generator.writeNumber((Float) value);
      } else if (value instanceof BigDecimal) {
         generator.writeNumber((BigDecimal) value);
      } else {
         generator.writeNumber(value.toString());
      }
   }
}
