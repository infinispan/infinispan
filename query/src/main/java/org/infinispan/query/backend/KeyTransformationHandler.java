/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.backend;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheException;
import org.infinispan.query.DefaultTransformer;
import org.infinispan.query.Transformable;
import org.infinispan.query.Transformer;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.logging.Log;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.UUID;

/**
 * This transforms arbitrary keys to a String which can be used by Lucene as a document identifier, and vice versa.
 * <p/>
 * There are 2 approaches to doing so; one for SimpleKeys: Java primitives (and their object wrappers) and Strings, and
 * one for custom, user-defined types that could be used as keys.
 * <p/>
 * For SimpleKeys, users don't need to do anything, these keys are automatically transformed by this class.
 * <p/>
 * For user-defined keys, two options are supported. Types annotated with @Transformable, and declaring an appropriate {@link
 * org.infinispan.query.Transformer} implementation; and types for which a {@link org.infinispan.query.Transformer} has
 * been explicitly registered through KeyTransformationHandler.registerTransformer().
 *
 * @author Manik Surtani
 * @author Marko Luksa
 * @see org.infinispan.query.Transformable
 * @see org.infinispan.query.Transformer
 * @since 4.0
 */
public class KeyTransformationHandler {

   private static final Log log = LogFactory.getLog(KeyTransformationHandler.class, Log.class);

   private final Map<Class<?>, Class<? extends Transformer>> transformerTypes = ConcurrentMapFactory.makeConcurrentMap();

   public Object stringToKey(String s, ClassLoader classLoader) {
      char type = s.charAt(0);
      switch (type) {
         case 'S':
            // this is a normal String, but NOT a SHORT. For short see case 'x'.
            return s.substring(2);
         case 'I':
            // This is an Integer
            return Integer.valueOf(s.substring(2));
         case 'Y':
            // This is a BYTE
            return Byte.valueOf(s.substring(2));
         case 'L':
            // This is a Long
            return Long.valueOf(s.substring(2));
         case 'X':
            // This is a SHORT
            return Short.valueOf(s.substring(2));
         case 'D':
            // This is a Double
            return Double.valueOf(s.substring(2));
         case 'F':
            // This is a Float
            return Float.valueOf(s.substring(2));
         case 'B':
            // This is a Boolean. This is NOT the case for a BYTE. For a BYTE, see case 'y'.
            return Boolean.valueOf(s.substring(2));
         case 'C':
            // This is a Character
            return Character.valueOf(s.charAt(2));
         case 'U':
            // This is a java.util.UUID
            return UUID.fromString(s.substring(2));
         case 'T':
            // this is a custom transformable.
            int indexOfSecondDelimiter = s.indexOf(":", 2);
            String keyClassName = s.substring(2, indexOfSecondDelimiter);
            String keyAsString = s.substring(indexOfSecondDelimiter + 1);
            Transformer t = getCustomTransformer(keyClassName, classLoader);
            if (t == null) throw new CacheException("Cannot find an appropriate Transformer for key type " + keyClassName);
            return t.fromString(keyAsString);
      }
      throw new CacheException("Unknown type metadata " + type);
   }

   private Transformer getCustomTransformer(final String keyClassName, final ClassLoader classLoader) {
      Transformer t = null;
      // try and locate class
      Class<?> keyClass = null;
      try {
         keyClass = Util.loadClassStrict(keyClassName, classLoader);
      } catch (ClassNotFoundException e) {
         log.keyClassNotFound(keyClassName, e);
      }
      if (keyClass != null) {
         t = getTransformer(keyClass);
      }
      return t;
   }

   public String keyToString(Object key) {
      // this string should be in the format of
      // "<TYPE>:(TRANSFORMER):<KEY>"
      // e.g.:
      //   "S:my string key"
      //   "I:75"
      //   "D:5.34"
      //   "B:f"
      //   "T:com.myorg.MyTransformer:STRING_GENERATED_BY_MY_TRANSFORMER"

      char prefix = ' ';

      // First going to check if the key is a primitive or a String. Otherwise, check if it's a transformable.
      // If none of those conditions are satisfied, we'll throw an Exception.

      Transformer tf = null;

      if (isStringOrPrimitive(key)) {
         // Using 'X' for Shorts and 'Y' for Bytes because 'S' is used for Strings and 'B' is being used for Booleans.


         if (key instanceof String)
            prefix = 'S';
         else if (key instanceof Integer)
            prefix = 'I';
         else if (key instanceof Boolean)
            prefix = 'B';
         else if (key instanceof Long)
            prefix = 'L';
         else if (key instanceof Float)
            prefix = 'F';
         else if (key instanceof Double)
            prefix = 'D';
         else if (key instanceof Short)
            prefix = 'X';
         else if (key instanceof Byte)
            prefix = 'Y';
         else if (key instanceof Character)
            prefix = 'C';
         else if (key instanceof UUID)
            prefix = 'U';
         return prefix + ":" + key;

      } else if ((tf = getTransformer(key.getClass())) != null) {
         // There is a bit more work to do for this case.
         return "T:" + key.getClass().getName() + ":" + tf.toString(key);
      } else
         throw new IllegalArgumentException("Indexing only works with entries keyed on Strings, primitives " +
               "and classes that have the @Transformable annotation - you passed in a " + key.getClass().toString() +
               ". Alternatively, see org.infinispan.query.SearchManager#registerKeyTransformer");
   }

   private boolean isStringOrPrimitive(Object key) {

      // we support String and JDK primitives and their wrappers.
      if (key instanceof String ||
            key instanceof Integer ||
            key instanceof Long ||
            key instanceof Float ||
            key instanceof Double ||
            key instanceof Boolean ||
            key instanceof Short ||
            key instanceof Byte ||
            key instanceof Character ||
            key instanceof UUID
            )
         return true;

      return false;
   }

   /**
    * Retrieves a {@link org.infinispan.query.Transformer} instance for this key.  If the key is not
    * {@link org.infinispan.query.Transformable} and no transformer has been registered for the key's class,
    * null is returned.
    *
    * @param keyClass key class to analyze
    * @return a Transformer for this key, or null if the key type is not properly annotated.
    */
   private Transformer getTransformer(Class<?> keyClass) {
      Class<?> transformerClass = getTransformerClass(keyClass);
      if (transformerClass != null)
         return instantiate(transformerClass);
      return null;
   }

   private Class<? extends Transformer> getTransformerClass(Class<?> keyClass) {
      Class<? extends Transformer> transformerClass = transformerTypes.get(keyClass);
      if (transformerClass == null) {
         transformerClass = getTransformerClassFromAnnotation(keyClass);
         if (transformerClass != null) {
             if (transformerClass.equals(DefaultTransformer.class)) {
                log.typeIsUsingDefaultTransformer(keyClass);
             }
             registerTransformer(keyClass, transformerClass);
         }
      }
      return transformerClass;
   }

   private Class<? extends Transformer> getTransformerClassFromAnnotation(Class<?> keyClass) {
      Transformable annotation = keyClass.getAnnotation(Transformable.class);
      if (annotation!=null) {
         return annotation.transformer();
      }
      return null;
   }

   private Transformer instantiate(Class<?> transformerClass) {
      try {
         // The cast should not be necessary but it's a workaround for a compiler bug.
         return (Transformer) transformerClass.newInstance();
      } catch (Exception e) {
         log.couldNotInstantiaterTransformerClass(transformerClass, e);
         return null;
      }
   }

   /**
    * Registers a {@link org.infinispan.query.Transformer} for the supplied key class.
    * @param keyClass the key class for which the supplied transformerClass should be used
    * @param transformerClass the transformer class to use for the supplied key class
    */
   public void registerTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      transformerTypes.put(keyClass, transformerClass);
   }

   /**
    * Gets the KeyTransformationHandler instance used by the supplied cache.
    * @param cache the cache for which we want to get the KeyTransformationHandler instance
    * @return a KeyTransformationHandler instance
    */
   public static KeyTransformationHandler getInstance(AdvancedCache<?, ?> cache) {
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      return queryInterceptor.getKeyTransformationHandler();
   }

}
