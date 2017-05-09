package org.infinispan.scripting.impl;

import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class DataTypedCacheManager extends AbstractDelegatingEmbeddedCacheManager {

   private static final Log log = LogFactory.getLog(DataTypedCacheManager.class, Log.class);

   final DataType dataType;
   final Optional<Marshaller> marshaller;

   public DataTypedCacheManager(DataType dataType, Optional<Marshaller> marshaller, EmbeddedCacheManager cm) {
      super(cm);
      this.dataType = dataType;
      this.marshaller = marshaller;
   }

   @Override
   public <K, V> Cache<K, V> getCache() {
      throw log.scriptsCanOnlyAccessNamedCaches();
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      Configuration cfg = super.getCacheConfiguration(cacheName);
      return cfg != null && cfg.compatibility().enabled()
            ? super.getCache(cacheName)
            : new DataTypedCache<>(this, super.getCache(cacheName));
   }

}
