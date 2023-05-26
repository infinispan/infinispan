package org.infinispan.multimap.internal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.multimap.impl.ExternalizerIds;

/**
 * A wrapper around {@link DataConversion} for multimap implementations.
 * </p>
 * This class is used to convert the keys and values of the multimap to the storage format and back again.
 * Some implementations must be aware of the actual contents for verification purposes, for example, a
 * <code>hashCode</code> for inserting a map.
 * </p>
 * The converter uses the {@link DataConversion} available on the underlying {@link Cache} which holds the
 * multimap. The conversion only applies in case hte cache is an {@link EncoderCache}.
 *
 * @param <K>: The raw type of the multimap key.
 * @param <V>: The raw type of the multimap values.
 * @since 15.0
 * @author José Bolina
 */
public class MultimapDataConverter<K, V> implements InjectableComponent {
   public static final AdvancedExternalizer<MultimapDataConverter> EXTERNALIZER = new Externalizer();

   private final DataConversion keyConversion;
   private final DataConversion valueConversion;

   public MultimapDataConverter(Cache<?, ?> cache) {
      if (cache instanceof EncoderCache) {
         EncoderCache<?, ?> encoderCache = (EncoderCache<?, ?>) cache;
         keyConversion = encoderCache.getKeyDataConversion();
         valueConversion = encoderCache.getValueDataConversion();
      } else {
         keyConversion = DataConversion.IDENTITY_KEY;
         valueConversion = DataConversion.IDENTITY_VALUE;
      }
   }

   private MultimapDataConverter(DataConversion keyConversion, DataConversion valueConversion) {
      this.keyConversion = keyConversion;
      this.valueConversion = valueConversion;
   }

   public Object convertKeyToStore(K key) {
      return keyConversion.toStorage(key);
   }

   public K convertKeyFromStore(Object key) {
      return (K) keyConversion.fromStorage(key);
   }

   public Object convertValueToStore(V value) {
      return valueConversion.toStorage(value);
   }

   public V convertValueFromStore(Object value) {
      return (V) valueConversion.fromStorage(value);
   }

   @Override
   public void inject(ComponentRegistry registry) {
      registry.wireDependencies(keyConversion);
      registry.wireDependencies(valueConversion);
   }

   public static class Externalizer implements AdvancedExternalizer<MultimapDataConverter> {

      @Override
      public Set<Class<? extends MultimapDataConverter>> getTypeClasses() {
         return Collections.singleton(MultimapDataConverter.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.MULTIMAP_CONVERTER;
      }

      @Override
      public void writeObject(ObjectOutput output, MultimapDataConverter object) throws IOException {
         output.writeObject(object.keyConversion);
         output.writeObject(object.valueConversion);
      }

      @Override
      public MultimapDataConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         DataConversion keyConversion = (DataConversion) input.readObject();
         DataConversion valueConversion = (DataConversion) input.readObject();
         return new MultimapDataConverter(keyConversion, valueConversion);
      }
   }
}
