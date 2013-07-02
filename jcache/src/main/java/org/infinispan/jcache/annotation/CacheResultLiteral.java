package org.infinispan.jcache.annotation;

import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheResolverFactory;
import javax.cache.annotation.CacheResult;
import javax.enterprise.util.AnnotationLiteral;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CacheResultLiteral extends AnnotationLiteral<CacheResult> implements CacheResult {

   public final static CacheResultLiteral INSTANCE = new CacheResultLiteral();

   private CacheResultLiteral() {
   }

   @Override
   public String cacheName() {
      return "";
   }

   @Override
   public boolean skipGet() {
      return false;
   }

   @Override
   public boolean cacheNull() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public Class<? extends CacheResolverFactory> cacheResolverFactory() {
      return CacheResolverFactory.class;
   }

   @Override
   public Class<? extends CacheKeyGenerator> cacheKeyGenerator() {
      return CacheKeyGenerator.class;
   }

   @Override
   public String exceptionCacheName() {
      return "";
   }

   @Override
   @SuppressWarnings("unchecked")
   public Class<? extends Throwable>[] cachedExceptions() {
      return new Class[0];
   }

   @Override
   @SuppressWarnings("unchecked")
   public Class<? extends Throwable>[] nonCachedExceptions() {
      return new Class[0];
   }

}
