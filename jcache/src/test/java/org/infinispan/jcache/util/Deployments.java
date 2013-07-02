package org.infinispan.jcache.util;

import org.infinispan.jcache.JCache;
import org.infinispan.jcache.annotation.CacheResultInterceptor;
import org.infinispan.jcache.annotation.CacheKeyInvocationContextFactory;
import org.infinispan.jcache.annotation.MethodMetaData;
import org.infinispan.jcache.annotation.CacheLookupHelper;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * Arquillian deployment utility class.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class Deployments {
   /**
    * The base deployment web archive. The CDI extension is packaged as an individual jar.
    */
   public static WebArchive baseDeployment() {
      return ShrinkWrap.create(WebArchive.class, "test.war")
         .addAsWebInfResource(Deployments.class.getResource("/beans.xml"), "beans.xml")
         .addAsLibrary(
            ShrinkWrap.create(JavaArchive.class, "infinispan-jcache-deployment.jar")
               .addPackage(CacheResultInterceptor.class.getPackage())
               .addPackage(CacheLookupHelper.class.getPackage())
               .addPackage(CacheKeyInvocationContextFactory.class.getPackage())
               .addPackage(MethodMetaData.class.getPackage())
               // .addAsManifestResource(JCache.class.getResource("/META-INF/beans.xml"), "beans.xml")
               .addAsManifestResource(JCache.class.getResource("/META-INF/services/javax.enterprise.inject.spi.Extension"),
                     "services/javax.enterprise.inject.spi.Extension")
         );
   }

}
