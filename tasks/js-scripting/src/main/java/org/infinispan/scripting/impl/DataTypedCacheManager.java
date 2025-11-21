package org.infinispan.scripting.impl;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.logging.LogFactory;

public final class DataTypedCacheManager extends AbstractDelegatingEmbeddedCacheManager {

   private static final Log log = LogFactory.getLog(DataTypedCacheManager.class, Log.class);

   private final MediaType scriptMediaType;
   private final Subject subject;

   DataTypedCacheManager(MediaType scriptMediaType, EmbeddedCacheManager cm, Subject subject) {
      super(cm);
      this.scriptMediaType = scriptMediaType;
      this.subject = subject;
   }

   @Override
   public <K, V> Cache<K, V> getCache() {
      throw log.scriptsCanOnlyAccessNamedCaches();
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      Cache<K, V> cache = super.getCache(cacheName);
      return cache.getAdvancedCache().withSubject(subject).withMediaType(scriptMediaType, scriptMediaType);
   }

}
