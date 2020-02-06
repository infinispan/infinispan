package org.infinispan.query.backend;

import static org.infinispan.query.logging.Log.CONTAINER;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.query.Transformable;
import org.infinispan.query.Transformer;
import org.infinispan.query.impl.DefaultTransformer;

/**
 * This transforms arbitrary keys to a String which can be used by Lucene as a document identifier, and vice versa.
 * <p>
 * There are 2 approaches to doing so; one for simple keys: Java primitives (and their object wrappers), byte[], Strings
 * and UUID, and one for custom, user-defined types that could be used as keys.
 * <p>
 * For simple keys, users don't need to do anything, these keys are automatically transformed by this class.
 * <p>
 * For user-defined keys, three options are supported. Types annotated with @Transformable, and declaring an appropriate
 * {@link org.infinispan.query.Transformer} implementation, types for which a {@link org.infinispan.query.Transformer}
 * has been explicitly registered through KeyTransformationHandler.registerTransformer() or through the indexing configuration
 * ({@link org.infinispan.configuration.cache.IndexingConfigurationBuilder#addKeyTransformer}).
 *
 * @author Manik Surtani
 * @author Marko Luksa
 * @see org.infinispan.query.Transformable
 * @see org.infinispan.query.Transformer
 * @since 4.0
 */
public final class KeyTransformationHandler {

   private final Map<Class<?>, Class<? extends Transformer>> transformerTypes = new ConcurrentHashMap<>();

   private final ClassLoader classLoader;

   /**
    * Constructs a KeyTransformationHandler for an indexed Cache.
    *
    * @param classLoader the classloader of the cache that owns this KeyTransformationHandler or {@code null} if the
    *                    thread context classloader is to be used (acceptable for testing only!)
    */
   public KeyTransformationHandler(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   /**
    * Converts a Lucene document id from string form back to the original object.
    *
    * @param s the string form of the key
    * @return the key object
    */
   public Object stringToKey(String s) {
      char type = s.charAt(0);
      int idx = s.lastIndexOf(":");
      switch (type) {
         case 'S':
            // this is a String, NOT a Short. For Short see case 'X'.
            return s.substring(2, idx);
         case 'I':
            // This is an Integer
            return Integer.valueOf(s.substring(2, idx));
         case 'Y':
            // This is a BYTE
            return Byte.valueOf(s.substring(2, idx));
         case 'L':
            // This is a Long
            return Long.valueOf(s.substring(2, idx));
         case 'X':
            // This is a SHORT
            return Short.valueOf(s.substring(2, idx));
         case 'D':
            // This is a Double
            return Double.valueOf(s.substring(2, idx));
         case 'F':
            // This is a Float
            return Float.valueOf(s.substring(2, idx));
         case 'B':
            // This is a Boolean, NOT a Byte. For Byte see case 'Y'.
            return Boolean.valueOf(s.substring(2, idx));
         case 'C':
            // This is a Character
            return Character.valueOf(s.charAt(2));
         case 'U':
            // This is a java.util.UUID
            return UUID.fromString(s.substring(2, idx));
         case 'A':
            // This is an array of bytes encoded as a Base64 string
            return Base64.getDecoder().decode(s.substring(2, idx));
         case 'T':
            // this is a custom Transformable or a type with a registered Transformer
            int indexOfSecondDelimiter = s.indexOf(':', 2);
            String keyClassName = s.substring(2, indexOfSecondDelimiter);
            String keyAsString = s.substring(indexOfSecondDelimiter + 1, idx);
            Transformer t = getTransformer(keyClassName);
            if (t != null) {
               return t.fromString(keyAsString);
            } else {
               throw CONTAINER.noTransformerForKey(keyClassName);
            }
      }
      throw new CacheException("Unknown key type metadata: " + type);
   }

   private Transformer getTransformer(String keyClassName) {
      Class<?> keyClass;
      try {
         keyClass = Util.loadClassStrict(keyClassName, classLoader);
      } catch (ClassNotFoundException e) {
         CONTAINER.keyClassNotFound(keyClassName, e);
         return null;
      }
      return getTransformer(keyClass);
   }

   /**
    * Stringify a key to encode the id and the infinispan segment field.
    *
    * @param key the key
    * @return a string form of the key
    */
   public String keyToString(Object key, int segmentId) {
      // This string should be in the format of:
      // "<TYPE>:<KEY>:<SEGMENT>" for internally supported types or "T:<KEY_CLASS>:<KEY>:<SEGMENT>" for custom types
      // e.g.:
      //   "S:my string key:1"
      //   "I:75:123"
      //   "D:5.34:12"
      //   "B:f:12"
      //   "T:com.myorg.MyType:STRING_GENERATED_BY_TRANSFORMER_FOR_MY_TYPE:20"

      // First going to check if the key is a primitive or a String. Otherwise, check if it's a transformable.
      // If none of those conditions are satisfied, we'll throw a CacheException.

      // Using 'X' for Shorts and 'Y' for Bytes because 'S' is used for Strings and 'B' is being used for Booleans.
      if (key instanceof byte[])
         return "A:" + Base64.getEncoder().encodeToString((byte[]) key) + ":" + segmentId;  //todo [anistor] need to profile Base64 versus simple hex encoding of the raw bytes
      if (key instanceof String)
         return "S:" + key + ":" + segmentId;
      else if (key instanceof Integer)
         return "I:" + key + ":" + segmentId;
      else if (key instanceof Boolean)
         return "B:" + key + ":" + segmentId;
      else if (key instanceof Long)
         return "L:" + key + ":" + segmentId;
      else if (key instanceof Float)
         return "F:" + key + ":" + segmentId;
      else if (key instanceof Double)
         return "D:" + key + ":" + segmentId;
      else if (key instanceof Short)
         return "X:" + key + ":" + segmentId;
      else if (key instanceof Byte)
         return "Y:" + key + ":" + segmentId;
      else if (key instanceof Character)
         return "C:" + key + ":" + segmentId;
      else if (key instanceof UUID)
         return "U:" + key + ":" + segmentId;
      else {
         Transformer t = getTransformer(key.getClass());
         if (t != null) {
            return "T:" + key.getClass().getName() + ":" + t.toString(key) + ":" + segmentId;
         } else {
            throw CONTAINER.noTransformerForKey(key.getClass().getName());
         }
      }
   }

   /**
    * Retrieves a {@link org.infinispan.query.Transformer} instance for this key.  If the key is not {@link
    * org.infinispan.query.Transformable} and no transformer has been registered for the key's class, null is returned.
    *
    * @param keyClass key class to analyze
    * @return a Transformer for this key, or null if the key type is not properly annotated.
    */
   private Transformer getTransformer(Class<?> keyClass) {
      Class<? extends Transformer> transformerClass = getTransformerClass(keyClass);
      if (transformerClass != null) {
         try {
            return transformerClass.newInstance();
         } catch (Exception e) {
            CONTAINER.couldNotInstantiaterTransformerClass(transformerClass, e);
         }
      }
      return null;
   }

   private Class<? extends Transformer> getTransformerClass(Class<?> keyClass) {
      Class<? extends Transformer> transformerClass = transformerTypes.get(keyClass);
      if (transformerClass == null) {
         Transformable transformableAnnotation = keyClass.getAnnotation(Transformable.class);
         transformerClass = transformableAnnotation != null ? transformableAnnotation.transformer() : null;
         if (transformerClass != null) {
            if (transformerClass == DefaultTransformer.class) {
               CONTAINER.typeIsUsingDefaultTransformer(keyClass);
            }
            registerTransformer(keyClass, transformerClass);
         }
      }
      return transformerClass;
   }

   /**
    * Registers a {@link org.infinispan.query.Transformer} for the supplied key class.
    *
    * @param keyClass         the key class for which the supplied transformerClass should be used
    * @param transformerClass the transformer class to use for the supplied key class
    */
   public void registerTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      transformerTypes.put(keyClass, transformerClass);
   }
}
