package test.infinispan.integration.embedded;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;
import test.infinispan.integration.AbstractSpringSessionTCK;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = EmbeddedSessionApp.class,
      webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT,
      properties = "spring.main.banner-mode=off")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class EmbeddedSpringSessionTest extends AbstractSpringSessionTCK {

   @Autowired
   CacheManager cacheManager;

   @Test
   public void testCacheManagerBean() {
      assertNotNull(cacheManager);
      assertInstanceOf(SpringEmbeddedCacheManager.class, cacheManager);
      EmbeddedCacheManager nativeCacheManager = ((SpringEmbeddedCacheManager) cacheManager).getNativeCacheManager();
      assertNotNull(nativeCacheManager);
   }
}
