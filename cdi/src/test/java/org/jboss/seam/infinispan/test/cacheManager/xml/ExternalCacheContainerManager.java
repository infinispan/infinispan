package org.jboss.seam.infinispan.test.cacheManager.xml;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.seam.infinispan.CacheContainerManager;
import org.jboss.seam.solder.resourceLoader.Resource;

import javax.enterprise.inject.Specializes;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@Specializes
public class ExternalCacheContainerManager extends CacheContainerManager {

   /**
    * Create the Cache Container from XML,
    *
    * @param xml
    * @return
    * @throws IOException
    */
   public static CacheContainer createCacheContainer(InputStream xml)
         throws IOException {
      // Create the cache container from the XML config, and associate with the
      // producer fields
      EmbeddedCacheManager cacheManager = new DefaultCacheManager(xml);

      // Define the very-large and quick-very-large configuration, based on the
      // defaults
      cacheManager.defineConfiguration("very-large", cacheManager
            .getDefaultConfiguration().clone());

      Configuration quickVeryLargeConfiguration = cacheManager
            .getDefaultConfiguration().clone();
      quickVeryLargeConfiguration.fluent()
            .eviction()
            .wakeUpInterval(1l);
      cacheManager.defineConfiguration("quick-very-large",
                                       quickVeryLargeConfiguration);
      return cacheManager;
   }

   // Constructor for proxies only
   protected ExternalCacheContainerManager() {
   }

   @Inject
   public ExternalCacheContainerManager(@Resource("infinispan.xml") InputStream xml) throws IOException {
      super(createCacheContainer(xml));
   }

}
