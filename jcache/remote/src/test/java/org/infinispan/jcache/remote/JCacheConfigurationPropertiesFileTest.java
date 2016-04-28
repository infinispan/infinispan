package org.infinispan.jcache.remote;

import org.infinispan.commons.util.FileLookupFactory;
import org.testng.annotations.Test;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "jcache.remote.JCacheConfigurationPropertiesFileTest", groups = "functional")
public class JCacheConfigurationPropertiesFileTest {

   public void testPropertiesConfiguration() {
      Class<?> clazz = this.getClass();
      ClassLoader cl = clazz.getClassLoader();
      CachingProvider provider = Caching.getCachingProvider(cl);
      Properties testProperties = getProperties(cl);
      CacheManager cm = provider.getCacheManager(URI.create(clazz.getName()), cl);
      assertEquals(testProperties, cm.getProperties());
   }

   public Properties getProperties(ClassLoader cl) {
      InputStream is = FileLookupFactory.newInstance().lookupFile("hotrod-client.properties", cl);
      Properties testProperties = new Properties();
      try {
         testProperties.load(is);
         return testProperties;
      } catch (IOException e) {
         throw new AssertionError(e);
      }
   }

}
