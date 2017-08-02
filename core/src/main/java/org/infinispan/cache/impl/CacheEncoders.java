package org.infinispan.cache.impl;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingUtils;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * TODO: Refactor with ISPN-
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class CacheEncoders {

   public static final CacheEncoders EMPTY = new CacheEncoders();

   private final Encoder keyEncoder;
   private final Encoder valueEncoder;
   private final Wrapper keyWrapper;
   private final Wrapper valueWrapper;

   private CacheEncoders() {
      this.keyEncoder = null;
      this.keyWrapper = null;
      this.valueEncoder = null;
      this.valueWrapper = null;
   }

   private CacheEncoders(Encoder keyEncoder, Wrapper keyWrapper, Encoder valueEncoder, Wrapper valueWrapper) {
      this.keyEncoder = keyEncoder;
      this.keyWrapper = keyWrapper;
      this.valueEncoder = valueEncoder;
      this.valueWrapper = valueWrapper;
   }

   public static CacheEncoders create(Encoder keyEncoder, Wrapper keyWrapper, Encoder valueEncoder, Wrapper valueWrapper) {
      return new CacheEncoders(keyEncoder, keyWrapper, valueEncoder, valueWrapper);
   }

   public static CacheEncoders grabEncodersFromRegistry(EncoderRegistry encoderRegistry, EncodingClasses encodingClasses) {
      CacheEncoders cacheEncoders = new CacheEncoders(encoderRegistry.getEncoder(encodingClasses.getKeyEncoderClass()),
            encoderRegistry.getWrapper(encodingClasses.getKeyWrapperClass()),
            encoderRegistry.getEncoder(encodingClasses.getValueEncoderClass()),
            encoderRegistry.getWrapper(encodingClasses.getValueWrapperClass())
      );
      return cacheEncoders;
   }

   public Encoder getKeyEncoder() {
      return keyEncoder;
   }

   public Encoder getValueEncoder() {
      return valueEncoder;
   }

   public Wrapper getKeyWrapper() {
      return keyWrapper;
   }

   public Wrapper getValueWrapper() {
      return valueWrapper;
   }

   public boolean isKeyEncodingActive() {
      return keyEncoder != null && keyWrapper != null;
   }

   public boolean isValueEncodingActive() {
      return valueEncoder != null && valueWrapper != null;
   }

   public Object keyFromStorage(Object keyFromStorage) {
      if (keyFromStorage == null) return null;
      return isKeyEncodingActive() ? keyEncoder.fromStorage(keyWrapper.unwrap(keyFromStorage)) : keyFromStorage;
   }

   public Object keyToStorage(Object key) {
      return isKeyEncodingActive() && !EncodingUtils.isWrapped(key, keyWrapper) ? keyWrapper.wrap(keyEncoder.toStorage(key)) : key;
   }

   public Object valueFromStorage(Object valueFromStorage) {
      if (valueFromStorage == null) return null;
      return isValueEncodingActive() ? valueEncoder.fromStorage(valueWrapper.unwrap(valueFromStorage)) : valueFromStorage;
   }

   public Object valueToStorage(Object value) {
      return isValueEncodingActive() && !EncodingUtils.isWrapped(value, valueWrapper) ? valueWrapper.wrap(valueEncoder.toStorage(value)) : value;
   }
}
