package org.infinispan.jcache;

import static org.infinispan.jcache.util.JCacheTestingUtil.withCachingProvider;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.cache.Cache;
import javax.cache.configuration.MutableConfiguration;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfiguration;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jcache.JCacheConfigurationTest")
public class JCacheConfigurationTest extends AbstractInfinispanTest {

   public void testNamedCacheConfiguration() {
      withCacheManager(TestCacheManagerFactory.createCacheManager(true), cm -> {
         cm.defineConfiguration("oneCache", new ConfigurationBuilder().build());
         JCacheManager jCacheManager = new JCacheManager(URI.create("oneCacheManager"), cm, null);
         assertNotNull(jCacheManager.getCache("oneCache"));
      });
   }

   public void testJCacheManagerWherePathContainsFileSchemaAndAbsolutePath() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      withCachingProvider(provider -> {
         Properties properties = getProperties();
         try (JCacheManager jCacheManager = new JCacheManager(
               uri,
               provider.getClass().getClassLoader(),
               provider,
               properties)) {
            Cache<Object, Object> cache = jCacheManager.getCache("foo");
            assertNotNull(cache);
            assertEquals(10000, cache.unwrap(AdvancedCache.class).getCacheConfiguration().memory().maxCount());
         }
      });
   }

   private Properties getProperties() {
      Properties properties = new Properties();
      properties.put("test.max-count", "10000");
      return properties;
   }

   public class MyClassLoader extends URLClassLoader {
      MyClassLoader(URL[] urls, ClassLoader parent) {
         super(urls, parent);
      }

      public void addURL(URL url) {
         super.addURL(url);
      }

      @Override
      public URL getResource(String name) {
         return super.getResource(name);
      }
   }

   public void testJCacheManagerWithRealJarFileSchema() throws Exception {
      File sampleJarWithResourceFile = null;
      ClassLoader originalClassLoader = null;

      try {
         // given - creating sample jar file
         String tmpDir = CommonsTestingUtil.tmpDirectory(JCacheConfigurationTest.class);
         File tmpDirFile = new File(tmpDir);
         if (!tmpDirFile.exists()) {
            tmpDirFile.mkdirs();
         }
         String sampleJarWithResourcePathString = Paths.get(tmpDir, "sampleJarWithResource.jar").toString();
         sampleJarWithResourceFile = new File(sampleJarWithResourcePathString);
         String existingResourceToAddInJar = "infinispan_uri.xml";
         String targetPathInJar = "sample-dir-inside-jar/";
         createJar(sampleJarWithResourceFile, existingResourceToAddInJar, targetPathInJar);

         // given - adding jar file into classpath
         String fullTargetPath = targetPathInJar + existingResourceToAddInJar;

         originalClassLoader = Thread.currentThread().getContextClassLoader();
         MyClassLoader myClassLoader = new MyClassLoader(new URL[]{}, originalClassLoader);
         myClassLoader.addURL(new URI("jar:" + sampleJarWithResourceFile.toURI() + "!/").toURL());
         Thread.currentThread().setContextClassLoader(myClassLoader);

         // when
         URI resourceInsideJarUri = new URI("jar:" + sampleJarWithResourceFile.toURI() + "!" + fullTargetPath);
         withCachingProvider(provider -> {
            try (JCacheManager jCacheManager = new JCacheManager(
                  resourceInsideJarUri,
                  provider.getClass().getClassLoader(),
                  provider,
                  getProperties())) {

               // then
               assertNotNull(jCacheManager.getCache("foo"));
            }
         });
      } finally {
         // cleanup
         if (sampleJarWithResourceFile != null && sampleJarWithResourceFile.exists()) {
            sampleJarWithResourceFile.delete();
         }

         if (originalClassLoader != null) {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
         }
      }
   }

   private void createJar(File jarFile, String existingResourceToAddInJar, String targetPathInJar) throws IOException, URISyntaxException {
      Manifest manifest = new Manifest();
      manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
      JarOutputStream sampleJarOutputStream = new JarOutputStream(new FileOutputStream(jarFile), manifest);

      URI contentFileUri = Objects.requireNonNull(JCacheConfigurationTest.class.getClassLoader().getResource(existingResourceToAddInJar)).toURI();
      File contentFile = new File(contentFileUri);
      String fullTargetPath = targetPathInJar + contentFile.getName();

      addInJar(new File(contentFileUri), fullTargetPath, sampleJarOutputStream);

      sampleJarOutputStream.close();
   }

   public void testJCacheManagerWithWildcardCacheConfigurations() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      withCachingProvider(provider -> {
         try (JCacheManager jCacheManager =
                    new JCacheManager(uri, provider.getClass().getClassLoader(), provider, getProperties())) {
            Cache<Object, Object> wildcache1 = jCacheManager.createCache("wildcache1", new MutableConfiguration<>());
            org.infinispan.Cache unwrap = wildcache1.unwrap(org.infinispan.Cache.class);
            Configuration configuration = unwrap.getCacheConfiguration();
            assertEquals(10500, configuration.expiration().wakeUpInterval());
            assertEquals(11, configuration.expiration().lifespan());
            assertEquals(6, configuration.expiration().maxIdle());
         }
      });
   }

   private void addInJar(File contentFile, String targetFilePath, JarOutputStream jarOutputStream) throws IOException {
      if (contentFile.isDirectory()) {
         return;
      }

      JarEntry jarEntry = new JarEntry(targetFilePath.replace("\\", "/"));
      jarOutputStream.putNextEntry(jarEntry);
      jarEntry.setTime(contentFile.lastModified());

      try (BufferedInputStream contentFileBufferedInputStream = new BufferedInputStream(new FileInputStream(contentFile))) {
         byte[] buffer = new byte[1024];

         while (true) {
            int count = contentFileBufferedInputStream.read(buffer);
            if (count == -1) {
               break;
            }

            jarOutputStream.write(buffer, 0, count);
         }

         jarOutputStream.closeEntry();
      }
   }

   public void testProgrammaticGlobalConfiguration() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      Properties properties = getProperties();
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.addModule(PrivateGlobalConfigurationBuilder.class);
      properties.put(GlobalConfigurationBuilder.class.getName(), global);
      withCachingProvider(provider -> {
         try (JCacheManager jCacheManager =
                    new JCacheManager(uri, provider.getClass().getClassLoader(), provider, properties)) {
            DefaultCacheManager defaultCacheManager = jCacheManager.unwrap(DefaultCacheManager.class);
            assertNotNull(defaultCacheManager.getCacheManagerConfiguration().module(PrivateGlobalConfiguration.class));
         }
      });
   }

   public void testProgrammaticConfigurationHolder() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      Properties properties = getProperties();
      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
      cbh.getGlobalConfigurationBuilder().addModule(PrivateGlobalConfigurationBuilder.class);
      properties.put(ConfigurationBuilderHolder.class.getName(), cbh);
      withCachingProvider(provider -> {
         try (JCacheManager jCacheManager =
                    new JCacheManager(uri, provider.getClass().getClassLoader(), provider, properties)) {
            DefaultCacheManager defaultCacheManager = jCacheManager.unwrap(DefaultCacheManager.class);
            assertNotNull(defaultCacheManager.getCacheManagerConfiguration().module(PrivateGlobalConfiguration.class));
         }
      });
   }

   public void testProgrammaticCacheConfiguration() throws Exception {
      URI uri = JCacheConfigurationTest.class.getClassLoader().getResource("infinispan_uri.xml").toURI();
      Properties properties = getProperties();
      Function<String, Configuration> f = (s) -> new ConfigurationBuilder().memory().maxCount(1234l).build();
      properties.put(JCacheManager.CACHE_CONFIGURATION_FUNCTION, f);
      withCachingProvider(provider -> {
         try (JCacheManager jCacheManager =
                    new JCacheManager(uri, provider.getClass().getClassLoader(), provider, properties)) {
            Cache<Object, Object> cache = jCacheManager.createCache("cache", new MutableConfiguration<>());
            org.infinispan.Cache unwrap = cache.unwrap(org.infinispan.Cache.class);
            Configuration configuration = unwrap.getCacheConfiguration();
            assertEquals(1234l, configuration.memory().maxCount());
         }
      });
   }
}
