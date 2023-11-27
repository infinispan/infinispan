package org.infinispan.jcache.remote;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(testName = "jcache.remote.JCacheConfigurationPropertiesFileTest", groups = "functional")
public class JCacheConfigurationPropertiesFileTest extends AbstractInfinispanTest {

   public void testPropertiesConfiguration() {
      Class<?> clazz = this.getClass();
      ClassLoader cl = clazz.getClassLoader();
      CachingProvider provider = Caching.getCachingProvider(cl);
      Properties testProperties = getProperties(cl, "hotrod-client.properties");
      try (CacheManager cm = provider.getCacheManager(URI.create(clazz.getName()), cl)) {
         assertEquals(testProperties, cm.getProperties());
      }
   }

   public void testPropertiesURIConfiguration() throws URISyntaxException {
      Class<?> clazz = this.getClass();
      ClassLoader cl = clazz.getClassLoader();
      CachingProvider provider = Caching.getCachingProvider(cl);
      Properties testProperties = getProperties(cl, "hotrod-client-custom.properties");
      try (CacheManager cm = provider.getCacheManager(cl.getResource("hotrod-client-custom.properties").toURI(), cl)) {
         assertEquals(testProperties, cm.getProperties());
      }
   }

   public Properties getProperties(ClassLoader cl, String name) {
      try (InputStream is = FileLookupFactory.newInstance().lookupFile(name, cl)) {
         Properties testProperties = new Properties();
         testProperties.load(is);
         return testProperties;
      } catch (IOException e) {
         throw new AssertionError(e);
      }
   }
}
