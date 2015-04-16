package org.infinispan.jcache;

import org.infinispan.jcache.AbstractTwoCachesAnnotationsTest;
import org.infinispan.jcache.JCacheAnnotatedClass;
import org.infinispan.jcache.annotation.InjectedCachePutInterceptor;
import org.infinispan.jcache.embedded.JCache;
import org.infinispan.jcache.util.JCacheTestingUtil;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;
import javax.inject.Inject;

import static org.infinispan.jcache.util.JCacheTestingUtil.getCache;

/**
 * @author Matej Cimbora
 */
@RunWith(Arquillian.class)
public class JCacheTwoCachesAnnotationsTest extends AbstractTwoCachesAnnotationsTest {

   @Deployment
   public static JavaArchive createDeployment() {
      return ShrinkWrap.create(JavaArchive.class).addPackage(JCacheAnnotatedClass.class.getPackage()).addPackage(JCache.class.getPackage())
            .addPackage(InjectedCachePutInterceptor.class.getPackage()).addPackage(CacheConfig.class.getPackage()).addPackage(JCacheTestingUtil.class.getPackage())
            .addAsResource(JCacheTwoCachesAnnotationsTest.class.getResource("/beans.xml"), "beans.xml");
   }

   @Inject
   private JCacheAnnotatedClass jCacheAnnotatedClass;

   @Override
   public JCacheAnnotatedClass getJCacheAnnotatedClass() {
      return jCacheAnnotatedClass;
   }

   @Override
   public Cache getCache1(CachingProvider provider) {
      Cache cache = getCache(provider, JCacheTwoCachesAnnotationsTest.class, "dist-annotations-1.xml", "testAnnotationCache");
      cache.clear();
      return cache;
   }

   @Override
   public Cache getCache2(CachingProvider provider) {
      Cache cache = getCache(provider, JCacheTwoCachesAnnotationsTest.class, "dist-annotations-2.xml", "testAnnotationCache");
      cache.clear();
      return cache;
   }

}
