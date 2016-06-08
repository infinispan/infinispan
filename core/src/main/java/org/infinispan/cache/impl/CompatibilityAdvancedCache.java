package org.infinispan.cache.impl;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Collection;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.infinispan.compat.DoubleTypeConverter;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Advanced cache that converts values based on operation type before passing to the underlying cache and subsequently
 * converts the values to the original type for responses.
 * @author wburns
 * @since 9.0
 */
public class CompatibilityAdvancedCache<K, V> extends TypeConverterDelegatingAdvancedCache<K, V> {
   private TypeConverter<K, V, K, V> hotRodConverter;
   private TypeConverter<K, V, K, V> memcachedConverter;
   private TypeConverter<K, V, K, V> embeddedConverter;

   public CompatibilityAdvancedCache(AdvancedCache<K, V> cache, Marshaller marshaller, TypeConverter converter) {
      super(cache, c -> new CompatibilityAdvancedCache<>(c, marshaller, converter), converter);
      Collection<TypeConverter> converters = ServiceFinder.load(TypeConverter.class);
      for (TypeConverter foundConverter : converters) {
         // We assume these don't produce byte[] values when boxing
         if (foundConverter.supportsInvocation(Flag.OPERATION_HOTROD)) {
            hotRodConverter = setConverterMarshaller(new DoubleTypeConverter<>(foundConverter, converter), marshaller);
         } else if (foundConverter.supportsInvocation(Flag.OPERATION_MEMCACHED)) {
            memcachedConverter = setConverterMarshaller(new DoubleTypeConverter<>(foundConverter, converter), marshaller);
         }
      }
      embeddedConverter = setConverterMarshaller(new DoubleTypeConverter<>(new EmbeddedTypeConverter(), converter), marshaller);
   }

   private static TypeConverter setConverterMarshaller(TypeConverter converter, Marshaller marshaller) {
      if (marshaller != null)
         converter.setMarshaller(marshaller);

      return converter;
   }

   @Override
   protected TypeConverter getConverter() {
      Cache<K, V> cache = this.cache;
      while (cache instanceof AbstractDelegatingCache && !(cache instanceof DecoratedCache)) {
         cache = ((AbstractDelegatingCache<K, V>) cache).getDelegate();
      }
      if (cache instanceof DecoratedCache) {
         long flags = ((DecoratedCache<K, V>) cache).getFlagsBitSet();
         if (EnumUtil.containsAny(flags, FlagBitSets.OPERATION_HOTROD)) {
            return hotRodConverter;
         } else if (EnumUtil.containsAny(flags, FlagBitSets.OPERATION_MEMCACHED)) {
            return memcachedConverter;
         }
      }
      return embeddedConverter;
   }

   private static class EmbeddedTypeConverter implements TypeConverter<Object, Object, Object, Object> {
      private static final Log log = LogFactory.getLog(EmbeddedTypeConverter.class);

      private Marshaller marshaller;

      @Override
      public Object boxKey(Object key) {
         return key;
      }

      @Override
      public Object boxValue(Object value) {
         return value;
      }

      @Override
      public Object unboxKey(Object target) {
         return unboxValue(target);
      }

      @Override
      public Object unboxValue(Object target) {
         if (marshaller != null && target instanceof byte[]) {
            try {
               return marshaller.objectFromByteBuffer((byte[]) target);
            } catch (Exception e) {
               throw new CacheException("Unable to unmarshall return value");
            }
         }

         if (target instanceof byte[]) {
            // Try standard deserialization
            try {
               ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream((byte[]) target));
               return ois.readObject();
            } catch (Exception ee) {
               if (log.isDebugEnabled())
                  log.debugf("Standard deserialization not in use for %s", Util.printArray((byte[]) target));
            }
         }

         return target;
      }

      @Override
      public boolean supportsInvocation(Flag flag) {
         return false;
      }

      @Override
      public void setMarshaller(Marshaller marshaller) {
         this.marshaller = marshaller;
      }

   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      AdvancedCache<K, V> returned = super.withFlags(flags);
      if (returned != this && returned instanceof CompatibilityAdvancedCache) {
         CompatibilityAdvancedCache cac = (CompatibilityAdvancedCache) returned;
         cac.hotRodConverter = this.hotRodConverter;
         cac.memcachedConverter = this.memcachedConverter;
         cac.embeddedConverter = this.embeddedConverter;
      }
      return returned;
   }
}
