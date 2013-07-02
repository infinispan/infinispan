package org.infinispan.jcache.annotation;

import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheRemoveEntry;
import javax.cache.annotation.CacheResolverFactory;
import javax.enterprise.util.AnnotationLiteral;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CacheRemoveEntryLiteral extends AnnotationLiteral<CacheRemoveEntry> implements CacheRemoveEntry {

   public final static CacheRemoveEntryLiteral INSTANCE = new CacheRemoveEntryLiteral();

   private CacheRemoveEntryLiteral() {
   }

   @Override
   public String cacheName() {
      return "";
   }

   @Override
   public boolean afterInvocation() {
      return false;
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
   @SuppressWarnings("unchecked")
   public Class<? extends Throwable>[] evictFor() {
      return new Class[0];
   }

   @Override
   @SuppressWarnings("unchecked")
   public Class<? extends Throwable>[] noEvictFor() {
      return new Class[0];
   }

}
