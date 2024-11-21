package org.infinispan.client.hotrod.admin;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.File;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.RemoteCacheCreateOnAccessTest")
public class RemoteCacheCreateOnAccessTest extends MultiHotRodServersTest {
   char serverId;
   boolean clear = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      serverId = 'A';
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(2, builder);
   }

   @Override
   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      return addStatefulHotRodServer(builder, serverId++);
   }

   protected boolean isShared() {
      return false;
   }

   protected HotRodServer addStatefulHotRodServer(ConfigurationBuilder builder, char id) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName() + File.separator + id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      gcb.globalState().enable().persistentLocation(stateDirectory).
            configurationStorage(ConfigurationStorage.OVERLAY);
      if (isShared()) {
         String sharedDirectory = tmpDirectory(this.getClass().getSimpleName() + File.separator + "COMMON");
         gcb.globalState().sharedPersistentLocation(sharedDirectory);
      } else {
         gcb.globalState().sharedPersistentLocation(stateDirectory);
      }
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(gcb, builder);
      cm.defineConfiguration("template", builder.build());
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      servers.add(server);
      return server;
   }


   public void createOnAccessTemplateProgrammatic() throws Throwable {
      String cacheName = "cache-from-template-programmatic";
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .socketTimeout(3000)
            .remoteCache(cacheName)
               .templateName("template");
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessTemplateProgrammaticWildcard() throws Throwable {
      String cacheName = "org.infinispan.cache-from-template-programmatic-wildcard";
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .socketTimeout(3000)
            .remoteCache("org.infinispan.cache-*")
            .templateName("template");
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessTemplateDeclarative() throws Throwable {
      String cacheName = "cache-from-template-declarative";
      Properties properties = new Properties();
      properties.put(ConfigurationProperties.SO_TIMEOUT, "3000");
      properties.put(ConfigurationProperties.CACHE_PREFIX + cacheName + ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX, "template");
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .withProperties(properties);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessTemplateDeclarativeWildcard() throws Throwable {
      String cacheName = "org.infinispan.cache-from-template-declarative";
      Properties properties = new Properties();
      properties.put(ConfigurationProperties.SO_TIMEOUT, "3000");
      properties.put(ConfigurationProperties.CACHE_PREFIX + "[org.infinispan.cache*]" + ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX, "template");
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .withProperties(properties);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessConfigurationProgrammatic() throws Throwable {
      String cacheName = "cache-from-config-programmatic";
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .socketTimeout(3000)
            .remoteCache(cacheName)
            .configuration(xml);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessConfigurationDeclarative() throws Throwable {
      String cacheName = "cache-from-config-declarative";
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      Properties properties = new Properties();
      properties.put(ConfigurationProperties.SO_TIMEOUT, "3000");
      properties.put(ConfigurationProperties.CACHE_PREFIX + cacheName + ConfigurationProperties.CACHE_CONFIGURATION_SUFFIX, xml);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .withProperties(properties);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessConfigurationURIProgrammatic() throws Throwable {
      String cacheName = "cache-from-config-uri-programmatic";
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      File file = new File(String.format("target/test-classes/%s-hotrod-client.properties", cacheName));
      try (Writer w = Files.newBufferedWriter(file.toPath())) {
         w.write(xml);
      }
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .socketTimeout(3000)
            .remoteCache(cacheName)
            .configurationURI(file.toURI());
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessConfigurationURIDeclarative() throws Throwable {
      createOnAccessConfigurationURI("uri-with-scheme", () -> Thread.currentThread().getContextClassLoader().getResource("uri-with-scheme-hotrod-client.properties").toString());
   }

   public void createOnAccessConfigurationSchemelessURIDeclarative() throws Throwable {
      createOnAccessConfigurationURI("uri-without-scheme", () -> "uri-without-scheme-hotrod-client.properties");
   }

   private void createOnAccessConfigurationURI(String cacheName, Supplier<String> uri) throws Throwable {
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      File file = new File(String.format("target/test-classes/%s-hotrod-client.properties", cacheName));
      try (Writer w = Files.newBufferedWriter(file.toPath())) {
         w.write(xml);
      }
      Properties properties = new Properties();
      properties.put(ConfigurationProperties.SO_TIMEOUT, "3000");
      properties.put(ConfigurationProperties.CACHE_PREFIX + cacheName + ConfigurationProperties.CACHE_CONFIGURATION_URI_SUFFIX, uri.get());
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .withProperties(properties);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }

   public void createOnAccessConfigurationProgrammaticAfterConstruction() throws Throwable {
      String cacheName = "cache-from-config-declarative";
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .addServer()
            .host(server(0).getHost())
            .port(server(0).getPort())
            .socketTimeout(3000);
      try (RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build())) {
         remoteCacheManager.getConfiguration().addRemoteCache(cacheName, c -> c.configuration(xml));
         RemoteCache<String, String> cache = remoteCacheManager.getCache(cacheName);
         cache.put("a", "a");
      }
   }
}
