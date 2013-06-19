package org.infinispan.jcache.annotation;

import javax.cache.spi.AnnotationProvider;

/**
 * JCache {@link javax.cache.spi.AnnotationProvider} implementation. This implementation is
 * used to say that we support the annotations part of JCache specification.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@SuppressWarnings("unused")
public class CacheAnnotationProvider implements AnnotationProvider {

   @Override
   public boolean isSupported() {
      return true;
   }

}
