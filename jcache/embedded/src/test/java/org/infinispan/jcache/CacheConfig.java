package org.infinispan.jcache;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.io.IOException;
import java.io.InputStream;

public class CacheConfig {

   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager defaultClusteredCacheManager() throws IOException {
      InputStream inputStream = CacheConfig.class.getClassLoader().getResourceAsStream("dist-annotations-1.xml");
      return new DefaultCacheManager(inputStream, true);
   }
}