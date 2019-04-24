package org.infinispan.server.logging;

import static javax.json.stream.JsonGenerator.PRETTY_PRINTING;

import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;

import org.jboss.logmanager.formatters.JsonFormatter;

/**
 * A clone of org.jboss.logmanager.formatters.JsonFormatter. This changes the initial impl. to remove some unwanted
 * keys. This cannot be done via configuration. Subclassing/overriding is also dodgy because of private fields.
 * <p>
 * After we decide which exact format is ok for Red Hat Enterprise Common Logging / Fluentd we can hardcode JSON
 * generation and remove this code (extend directly from ExtFormatter) to avoid as much object creation and hashmap
 * lookups as possible.
 *
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 */
@SuppressWarnings("unused")
public final class LightJsonFormatter extends JsonFormatter {

   private static final Set<String> bannedKeys = new HashSet<>();

   // the keys we do not want to log, but StructuredFormatter would log them by default :(
   static {
      bannedKeys.add(Key.LOGGER_CLASS_NAME.getKey());
      bannedKeys.add(Key.MDC.getKey());
      bannedKeys.add(Key.NDC.getKey());
   }

   // these fields duplicate the ones in superclass!
   private final Map<String, Object> config = new HashMap<>();
   // needs to be mutated, needs synchronized access
   private JsonGeneratorFactory factory = Json.createGeneratorFactory(config);

   public LightJsonFormatter() {
      super();
   }

   public LightJsonFormatter(String keyOverrides) {
      super(keyOverrides);
   }

   public LightJsonFormatter(Map<Key, String> keyOverrides) {
      super(keyOverrides);
   }

   @Override
   public boolean isPrettyPrint() {
      synchronized (config) {
         return config.containsKey(PRETTY_PRINTING) ? (Boolean) config.get(PRETTY_PRINTING) : false;
      }
   }

   @Override
   public void setPrettyPrint(boolean prettyPrint) {
      synchronized (config) {
         if (prettyPrint) {
            config.put(JsonGenerator.PRETTY_PRINTING, true);
         } else {
            config.remove(JsonGenerator.PRETTY_PRINTING);
         }
         factory = Json.createGeneratorFactory(config);
      }
   }

   @Override
   protected Generator createGenerator(Writer writer) {
      JsonGeneratorFactory factory;
      synchronized (config) {
         factory = this.factory;
      }
      return new LightJsonGenerator(factory.createGenerator(writer));
   }

   private static final class LightJsonGenerator implements Generator {

      private final JsonGenerator generator;

      private LightJsonGenerator(JsonGenerator generator) {
         this.generator = generator;
      }

      @Override
      public Generator begin() {
         generator.writeStartObject();
         return this;
      }

      @Override
      public Generator add(String key, int value) {
         if (!bannedKeys.contains(key)) {
            generator.write(key, value);
         }
         return this;
      }

      @Override
      public Generator add(String key, long value) {
         if (!bannedKeys.contains(key)) {
            generator.write(key, value);
         }
         return this;
      }

      @Override
      public Generator add(String key, Map<String, ?> value) {
         if (!bannedKeys.contains(key)) {
            generator.writeStartObject(key);
            if (value != null) {
               for (Map.Entry<String, ?> entry : value.entrySet()) {
                  writeObject(entry.getKey(), entry.getValue());
               }
            }
            generator.writeEnd();
         }
         return this;
      }

      @Override
      public Generator add(String key, String value) {
         if (!bannedKeys.contains(key)) {
            if (value == null) {
               generator.writeNull(key);
            } else {
               generator.write(key, value);
            }
         }
         return this;
      }

      @Override
      public Generator startObject(String key) {
         if (key == null) {
            generator.writeStartObject();
         } else {
            generator.writeStartObject(key);
         }
         return this;
      }

      @Override
      public Generator endObject() {
         generator.writeEnd();
         return this;
      }

      @Override
      public Generator startArray(String key) {
         if (key == null) {
            generator.writeStartArray();
         } else {
            generator.writeStartArray(key);
         }
         return this;
      }

      @Override
      public Generator endArray() {
         generator.writeEnd();
         return this;
      }

      @Override
      public Generator end() {
         generator.writeEnd();
         generator.flush();
         generator.close();
         return this;
      }

      private void writeObject(String key, Object obj) {
         if (obj == null) {
            if (key == null) {
               generator.writeNull();
            } else {
               generator.writeNull(key);
            }
         } else if (obj instanceof Boolean) {
            Boolean value = (Boolean) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof Integer) {
            Integer value = (Integer) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof Long) {
            Long value = (Long) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof Double) {
            Double value = (Double) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof BigInteger) {
            BigInteger value = (BigInteger) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof BigDecimal) {
            BigDecimal value = (BigDecimal) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof String) {
            String value = (String) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else if (obj instanceof JsonValue) {
            JsonValue value = (JsonValue) obj;
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         } else {
            String value = String.valueOf(obj);
            if (key == null) {
               generator.write(value);
            } else {
               generator.write(key, value);
            }
         }
      }
   }
}
