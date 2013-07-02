package org.infinispan.jcache.annotation;

import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheResolverFactory;
import javax.enterprise.util.AnnotationLiteral;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CacheRemoveAllLiteral extends AnnotationLiteral<CacheRemoveAll> implements CacheRemoveAll {

   public final static CacheRemoveAllLiteral INSTANCE = new CacheRemoveAllLiteral();

   private CacheRemoveAllLiteral() {
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
