package org.infinispan.scripting.impl;

import java.util.Optional;

import javax.security.auth.Subject;

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
   final Subject subject;

   public DataTypedCacheManager(DataType dataType, Optional<Marshaller> marshaller, EmbeddedCacheManager cm, Subject subject) {
      super(cm);
      this.dataType = dataType;
      this.marshaller = marshaller;
      this.subject = subject;
   }

   @Override
   public <K, V> Cache<K, V> getCache() {
      throw log.scriptsCanOnlyAccessNamedCaches();
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      Configuration cfg = super.getCacheConfiguration(cacheName);
      Cache<K, V> cache = (Cache<K, V>) super.getCache(cacheName).getAdvancedCache().withSubject(subject);
      return cfg != null && cfg.compatibility().enabled()
            ? cache
            : new DataTypedCache<>(this, cache);
   }

}
