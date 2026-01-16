package org.infinispan.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.eq;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.Matchers;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.junit.JUnitThreadTrackerRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.badlogic.gdx.utils.SharedLibraryLoadRuntimeException;

import party.iroiro.luajava.lua51.Lua51;
import party.iroiro.luajava.lua51.Lua51Natives;
import party.iroiro.luajava.util.GlobalLibraryLoader;

public class ProtocolInitializationTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   @Test
   public void testRespServerStartWithLua() {
      RespServer respServer = new RespServer();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager()) {
         @Override
         public void call() {
            RespServerConfigurationBuilder builder = new RespServerConfigurationBuilder();
            executeLifecycle(respServer, builder.build(), cm);
         }
      });
   }

   @Test
   public void testRespServerStartWithoutLua() {
      try (MockedStatic<GlobalLibraryLoader> mockedStatic = Mockito.mockStatic(GlobalLibraryLoader.class)) {
         mockedStatic.when(() -> GlobalLibraryLoader.load(eq("lua51")))
               .thenThrow(new SharedLibraryLoadRuntimeException("Unable to locate the library path"));

         RespServer respServer = new RespServer();
         withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager()) {
            @Override
            public void call() {
               RespServerConfigurationBuilder builder = new RespServerConfigurationBuilder();
               forceLuaNativeUnload(Lua51.class, "natives");
               forceLuaNativeUnload(Lua51Natives.class, "loaded");
               executeLifecycle(respServer, builder.build(), cm);
            }
         });
      }
   }

   @Test
   public void testTopologyCacheDefined() {
      HotRodServer hotRodServer = new HotRodServer();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createClusteredCacheManager()) {
         @Override
         public void call() {
            cm.defineConfiguration(HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX + "_hotrod", new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build());
            HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
            assertThatThrownBy(() -> executeLifecycle(hotRodServer, builder.build(), cm))
                  .isInstanceOf(CacheConfigurationException.class);
         }
      });
   }

   @SuppressWarnings("rawtypes")
   private <C extends ProtocolServerConfiguration> void executeLifecycle(ProtocolServer<C> server, C config, EmbeddedCacheManager cm) {
      try {
         server.start(config, cm);
         server.postStart();
      } finally {
         server.stop();
      }
   }

   private void forceLuaNativeUnload(Class<?> clazz, String fieldName) {
      Exception exception = null;
      try {
         Field nativesField = clazz.getDeclaredField(fieldName);
         nativesField.setAccessible(true);
         AtomicReference<?> reference = (AtomicReference<?>) nativesField.get(null);
         if (reference != null) {
            reference.set(null);
         } else {
            exception = new NullPointerException("Native method reference is null");
         }
      } catch (Exception e) {
         exception = e;
      }

      assumeThat(String.format("Failed unloading: %s#%s", clazz, fieldName), exception, Matchers.equalTo(null));
   }
}
