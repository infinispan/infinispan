package org.infinispan.server.test.junit4;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.scripting.ScriptingManager;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import net.spy.memcached.MemcachedClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestMethodRule implements TestRule {
   public static final int TIMEOUT = 10;
   private final InfinispanServerRule infinispanServerRule;
   private String methodName;
   private List<Closeable> resources;

   public InfinispanServerTestMethodRule(InfinispanServerRule infinispanServerRule) {
      this.infinispanServerRule = Objects.requireNonNull(infinispanServerRule);
   }

   public <T extends Closeable> T registerResource(T resource) {
      resources.add(resource);
      return resource;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before();
            try {
               methodName = description.getTestClass().getSimpleName() + "." + description.getMethodName();
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before() {
      resources = new ArrayList<>();
   }

   private void after() {
      if (resources != null) {
         resources.forEach(Util::close);
         resources.clear();
      }
   }

   public String getMethodName() {
      return getMethodName(null);
   }

   public String getMethodName(String qualifier) {
      String cacheName = "C" + methodName + (qualifier != null ? qualifier : "");
      try {
         MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
         byte[] digest = sha1.digest(cacheName.getBytes(StandardCharsets.UTF_8));
         return Util.toHexString(digest);
      } catch (NoSuchAlgorithmException e) {
         // Won't happen
         return null;
      }
   }

   public HotRod hotrod() {
      return new HotRod();
   }

   public Rest rest() {
      return new Rest();
   }

   public CounterManager getCounterManager() {
      RemoteCacheManager remoteCacheManager = registerResource(infinispanServerRule.newHotRodClient());
      return RemoteCounterManagerFactory.asCounterManager(remoteCacheManager);
   }

   public MemcachedClient getMemcachedClient() {
      return registerResource(infinispanServerRule.newMemcachedClient()).getClient();
   }

   public MBeanServerConnection getJmxConnection(int server) {
      return infinispanServerRule.getServerDriver().getJmxConnection(server);
   }

   public abstract class Base<S extends Base<S>> implements Self<S> {
      protected BasicConfiguration serverConfiguration = null;
      protected EnumSet<CacheContainerAdmin.AdminFlag> flags = EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class);
      protected CacheMode mode = null;
      protected String qualifier;

      public S withServerConfiguration(org.infinispan.configuration.cache.ConfigurationBuilder serverConfiguration) {
         if (mode != null) {
            throw new IllegalStateException("Cannot set server configuration and cache mode");
         }
         this.serverConfiguration = serverConfiguration.build();
         return self();
      }

      public S withServerConfiguration(XMLStringConfiguration xmlConfiguration) {
         if (mode != null) {
            throw new IllegalStateException("Cannot set server configuration and cache mode");
         }
         this.serverConfiguration = xmlConfiguration;
         return self();
      }

      public S withCacheMode(CacheMode mode) {
         if (serverConfiguration != null) {
            throw new IllegalStateException("Cannot set server configuration and cache mode");
         }
         this.mode = mode;
         return self();
      }

      public S withQualifier(String qualifier) {
         this.qualifier = qualifier;
         return self();
      }

      public S makeVolatile() {
         this.flags = EnumSet.of(CacheContainerAdmin.AdminFlag.VOLATILE);
         return self();
      }
   }

   public class HotRod extends Base<HotRod> {
      ConfigurationBuilder clientConfiguration = new ConfigurationBuilder();

      private HotRod() {
      }

      public HotRod withClientConfiguration(ConfigurationBuilder clientConfiguration) {
         this.clientConfiguration = clientConfiguration;
         return this;
      }

      public <K, V> RemoteCache<K, V> get() {
         RemoteCacheManager remoteCacheManager = registerResource(infinispanServerRule.newHotRodClient(clientConfiguration));
         String name = getMethodName(qualifier);
         return remoteCacheManager.getCache(name);
      }

      public <K, V> RemoteCache<K, V> create() {
         RemoteCacheManager remoteCacheManager = registerResource(infinispanServerRule.newHotRodClient(clientConfiguration));
         String name = getMethodName(qualifier);
         if (serverConfiguration != null) {
            return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, serverConfiguration);
         } else if (mode != null) {
            return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, "org.infinispan." + mode.name());
         } else {
            return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, "org.infinispan." + CacheMode.DIST_SYNC.name());
         }
      }

      @Override
      public HotRod self() {
         return this;
      }
   }

   public class Rest extends Base<Rest> {
      RestClientConfigurationBuilder clientConfiguration = new RestClientConfigurationBuilder();

      private Rest() {
      }

      public Rest withClientConfiguration(RestClientConfigurationBuilder clientConfiguration) {
         this.clientConfiguration = clientConfiguration;
         return this;
      }

      public RestClient get() {
         return registerResource(infinispanServerRule.newRestClient(clientConfiguration));
      }

      public RestClient get(int n) {
         return registerResource(infinispanServerRule.newRestClient(clientConfiguration, n));
      }

      public RestClient create() {
         RestClient restClient = get();
         String name = getMethodName(qualifier);
         CompletionStage<RestResponse> future;
         if (serverConfiguration != null) {
            RestEntity configEntity = RestEntity.create(MediaType.APPLICATION_XML, serverConfiguration.toXMLString(name));
            future = restClient.cache(name).createWithConfiguration(configEntity, flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
         } else if (mode != null) {
            future = restClient.cache(name).createWithTemplate("org.infinispan." + mode.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
         } else {
            future = restClient.cache(name).createWithTemplate("org.infinispan." + CacheMode.DIST_SYNC.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
         }
         RestResponse response = Exceptions.unchecked(() -> future.toCompletableFuture().get(TIMEOUT, TimeUnit.SECONDS));
         if (response.getStatus() != 200) {
            response.close();
            throw new RuntimeException("Could not obtain rest client = " + response.getStatus());
         } else {
            return restClient;
         }
      }

      @Override
      public Rest self() {
         return this;
      }
   }

   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache(ScriptingManager.SCRIPT_CACHE);
      try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(script)) {
         scriptCache.put(getMethodName(), CommonsTestingUtil.loadFileAsString(in));
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return getMethodName();
   }
}
