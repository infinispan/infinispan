package org.infinispan.server.websocket.json;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.server.websocket.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Json Object implementation. This is the only link between POJOs and particular JSON library implementation.
 *
 * @author Sebastian Laskawiec
 */
public class JsonObject implements Map<String, Object> {

   private static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private static final ObjectMapper objectMapper = new ObjectMapper();

   private final Map<String, Object> internalStructure;

   private JsonObject(Map<String, Object> internalStructure) {
      this.internalStructure = internalStructure;
   }

   public static JsonObject fromString(final String json) throws JsonConversionException {
      try {
         @SuppressWarnings("unchecked")
         Map<String, Object> readValue = objectMapper.readValue(json, Map.class);
         return new JsonObject(readValue);
      } catch (Exception e) {
         throw logger.unableToConvertFromStringToJson(json, e);
      }
   }

   public static JsonObject fromObject(final Object o) throws JsonConversionException {
      try {
         @SuppressWarnings("unchecked")
         Map<String, Object> convertValue = objectMapper.convertValue(o, Map.class);
         return new JsonObject(convertValue);
      } catch (Exception e) {
         throw logger.unableToConvertFromObjectToJson(o, e);
      }
   }

   public static JsonObject fromMap(Map<String, Object> map) {
      return new JsonObject(map);
   }

   public static JsonObject createNew() {
      return new JsonObject(new HashMap<String, Object>());
   }

   @Override
   public String toString() {
      try {
         return objectMapper.writeValueAsString(internalStructure);
      } catch (IOException e) {
         throw logger.unableToConvertFromJsonToString(e);
      }
   }

   @Override
   public int size() {
      return internalStructure.size();
   }

   @Override
   public boolean isEmpty() {
      return internalStructure.isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return internalStructure.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return internalStructure.containsValue(value);
   }

   @Override
   public Object get(Object key) {
      return internalStructure.get(key);
   }

   @Override
   public Object put(String key, Object value) {
      return internalStructure.put(key, value);
   }

   @Override
   public Object remove(Object key) {
      return internalStructure.remove(key);
   }

   @Override
   public void putAll(Map<? extends String, ?> m) {
      internalStructure.putAll(m);
   }

   @Override
   public void clear() {
      internalStructure.clear();
   }

   @Override
   public Set<String> keySet() {
      return internalStructure.keySet();
   }

   @Override
   public Collection<Object> values() {
      return internalStructure.values();
   }

   @Override
   public Set<Entry<String, Object>> entrySet() {
      return internalStructure.entrySet();
   }
}
