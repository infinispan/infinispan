package org.infinispan.jcache.annotation;

import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheResolverFactory;
import javax.enterprise.util.AnnotationLiteral;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CachePutLiteral extends AnnotationLiteral<CachePut> implements CachePut {

   public final static CachePutLiteral INSTANCE = new CachePutLiteral();

   private CachePutLiteral() {
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
   public Class<? extends Throwable>[] noCacheFor() {
      return new Class[0];
   }

   @Override
   public boolean cacheNull() {
      return false;
   }

   @Override
   @SuppressWarnings("unchecked")
   public Class<? extends Throwable>[] cacheFor() {
      return new Class[0];
   }

}
