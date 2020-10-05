package org.infinispan.cloudevents.impl;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Serialize cache events into cloudevents-encoded JSON.
 */
public class StructuredEventBuilder {
   public static final int VALIDATION_BUFFER_SIZE = 512;
   public static final String SPECVERSION = "specversion";
   public static final String SPEC_VERSION_10 = "1.0";
   public static final String ID = "id";
   public static final String SOURCE = "source";
   public static final String SUBJECT = "subject";
   public static final String TYPE = "type";
   public static final String TIME = "time";
   public static final String DATA = "data";
   public static final String DATACONTENTTYPE = "datacontenttype";

   public static final String INFINISPAN_SUBJECT_CONTENTTYPE = "orginfinispansubject_contenttype";
   public static final String INFINISPAN_SUBJECT_ISBASE64 = "orginfinispansubject_isbase64";
   public static final String INFINISPAN_DATA_ISBASE64 = "orginfinispandata_isbase64";
   public static final String INFINISPAN_ENTRYVERSION = "orginfinispanentryversion";

   Json json;
   private byte[] key;

   public StructuredEventBuilder() {
      json = Json.object();
      json.set(SPECVERSION, SPEC_VERSION_10);
   }

   public ProducerRecord<byte[], byte[]> toKafkaRecord(String topic) {
      return new ProducerRecord<>(topic, key, json.toString().getBytes(StandardCharsets.UTF_8));
   }

   public void setId(String id) {
      json.set(ID, id);
   }

   public void setSource(String source) {
      json.set(SOURCE, source);
   }

   public void setType(String type) {
      json.set(TYPE, type);
   }

   public void setTime(String time) {
      json.set(TIME, time);
   }


   public void setSubject(String subject, MediaType mediaType, boolean validUtf8) {
      json.set(SUBJECT, subject);
      if (!mediaType.equals(MediaType.APPLICATION_JSON)) {
         json.set(INFINISPAN_SUBJECT_CONTENTTYPE, mediaType);
         if (!validUtf8) {
            json.set(INFINISPAN_SUBJECT_ISBASE64, Json.factory().bool(true));
         }
      }
   }

   public void setData(byte[] data, MediaType dataMediaType, boolean validUtf8) {
      if (dataMediaType.equals(MediaType.APPLICATION_JSON)) {
         String string = new String(data, dataMediaType.getCharset());
         json.set(DATA, Json.factory().raw(string));
      } else {
         json.set(DATACONTENTTYPE, dataMediaType);
         String valueString;
         if (!validUtf8) {
            json.set(INFINISPAN_DATA_ISBASE64, Json.factory().bool(true));
            valueString = Base64.getEncoder().encodeToString(data);
         } else {
            valueString = new String(data, dataMediaType.getCharset());
         }
         json.set(DATA, Json.factory().string(valueString));
      }
   }

   public void setPrimitiveData(Object data) {
      json.set(DATA, Json.factory().make(data));
   }

   public void setEntryVersion(byte[] version) {
      json.set(INFINISPAN_ENTRYVERSION, Base64.getEncoder().encodeToString(version));
   }

   /**
    * Check if the value can be represented as JSON with just primitive types and arrays.
    * Lists, sets, and maps are not supported, because they would require us to check each element.
    */
   static boolean isJsonPrimitive(Class<?> valueClass) {
      return valueClass == String.class ||
             valueClass == Boolean.class ||
             Number.class.isAssignableFrom(valueClass) ||
             valueClass.isArray() && isJsonPrimitive(valueClass.getComponentType());
   }

   public void setKey(byte[] keyBytes) {
      this.key = keyBytes;
   }

}
