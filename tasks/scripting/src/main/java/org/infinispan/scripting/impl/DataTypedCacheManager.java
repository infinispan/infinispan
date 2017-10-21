package org.infinispan.scripting.impl;

import java.util.Optional;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.GenericJbossMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class DataTypedCacheManager extends AbstractDelegatingEmbeddedCacheManager {

   private static final Log log = LogFactory.getLog(DataTypedCacheManager.class, Log.class);

   final DataType dataType;
   final Optional<Marshaller> marshaller;
   final Subject subject;
   private final Class<? extends Encoder> encoderClass;

   public DataTypedCacheManager(DataType dataType, Optional<Marshaller> marshaller, EmbeddedCacheManager cm, Subject subject) {
      super(cm);
      this.dataType = dataType;
      this.marshaller = marshaller;
      this.subject = subject;
      this.encoderClass = dataType == DataType.UTF8 ? UTF8Encoder.class :
            marshaller.isPresent() ? GenericJbossMarshallerEncoder.class : IdentityEncoder.class;
   }

   @Override
   public <K, V> Cache<K, V> getCache() {
      throw log.scriptsCanOnlyAccessNamedCaches();
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      Cache<K, V> cache = super.getCache(cacheName);
      MemoryConfiguration memory = SecurityActions.getCacheConfiguration(cache).memory();
      if (memory.storageType() == StorageType.OBJECT) {
         return (AdvancedCache<K, V>) cache.getAdvancedCache().withEncoding(encoderClass).withSubject(subject);
      }
      return cache.getAdvancedCache();
   }

}
