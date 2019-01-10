package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.Flag.FORCE_RETURN_VALUE;
import static org.infinispan.client.hotrod.Flag.SKIP_INDEXING;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests if the {@link org.infinispan.client.hotrod.Flag#SKIP_INDEXING} flag is received on HotRod server.
 *
 * @author Tristan Tarrant
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(testName = "client.hotrod.SkipIndexingFlagTest", groups = "functional")
@CleanupAfterTest
public class SkipIndexingFlagTest extends SingleCacheManagerTest {

   private static final String KEY = "key";
   private static final String VALUE = "value";
   private FlagCheckCommandInterceptor commandInterceptor;
   private RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotRodServer;

   public void testPut() {
      performTest(RequestType.PUT);
   }

   public void testReplace() {
      performTest(RequestType.REPLACE);
   }

   public void testPutIfAbsent() {
      performTest(RequestType.PUT_IF_ABSENT);
   }

   public void testReplaceIfUnmodified() {
      performTest(RequestType.REPLACE_IF_UNMODIFIED);
   }

   public void testGet() {
      performTest(RequestType.GET);
   }

   public void testGetWithVersion() {
      performTest(RequestType.GET_WITH_VERSION);
   }

   public void testGetWithMetadata() {
      performTest(RequestType.GET_WITH_METADATA);
   }

   public void testRemove() {
      performTest(RequestType.REMOVE);
   }

   public void testRemoveIfUnmodified() {
      performTest(RequestType.REMOVE_IF_UNMODIFIED);
   }

   public void testContainsKey() {
      performTest(RequestType.CONTAINS);
   }

   public void testPutAll() {
      performTest(RequestType.PUT_ALL);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      Properties hotRodClientConf = new Properties();
      hotRodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
      super.teardown();
   }

   private void performTest(RequestType type) {
      commandInterceptor.expectSkipIndexingFlag = false;
      type.execute(remoteCache);

      commandInterceptor.expectSkipIndexingFlag = RequestType.expectsFlag(type);
      type.execute(remoteCache.withFlags(SKIP_INDEXING));
      type.execute(remoteCache.withFlags(SKIP_INDEXING, FORCE_RETURN_VALUE));
   }

   @BeforeClass(alwaysRun = true)
   private void injectCommandInterceptor() {
      if (remoteCache == null) {
         return;
      }
      for (CommandInterceptor commandInterceptor : cache.getAdvancedCache().getInterceptorChain()) {
         if (commandInterceptor instanceof FlagCheckCommandInterceptor) {
            this.commandInterceptor = (FlagCheckCommandInterceptor) commandInterceptor;
         }

      }

      this.commandInterceptor = new FlagCheckCommandInterceptor();
      cache.getAdvancedCache().addInterceptor(commandInterceptor, 1);
   }

   @AfterClass(alwaysRun = true)
   private void resetCommandInterceptor() {
      if (commandInterceptor != null) {
         commandInterceptor.expectSkipIndexingFlag = false;
      }
   }

   private static enum RequestType {
      PUT {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.put(KEY, VALUE);
         }
      },
      REPLACE {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.replace(KEY, VALUE);
         }
      },
      PUT_IF_ABSENT {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.putIfAbsent(KEY, VALUE);
         }
      },
      REPLACE_IF_UNMODIFIED {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.replaceWithVersion(KEY, VALUE, 0);
         }
      },
      GET {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.get(KEY);
         }
      },
      GET_WITH_VERSION {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.getVersioned(KEY);
         }
      },
      GET_WITH_METADATA {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.getWithMetadata(KEY);
         }
      },
      REMOVE {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.remove(KEY);
         }
      },
      REMOVE_IF_UNMODIFIED {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.removeWithVersion(KEY, 0);
         }
      },
      CONTAINS {
         @Override
         void execute(RemoteCache<String, String> cache) {
            cache.containsKey(KEY);
         }
      },
      PUT_ALL {
         @Override
         void execute(RemoteCache<String, String> cache) {
            Map<String, String> data = new HashMap<String, String>();
            data.put(KEY, VALUE);
            cache.putAll(data);
         }
      },
      ;

      private static boolean expectsFlag(RequestType type) {
         return type != CONTAINS && type != GET && type != GET_WITH_METADATA && type != GET_WITH_VERSION;
      }

      abstract void execute(RemoteCache<String, String> cache);
   }

   private static class FlagCheckCommandInterceptor extends BaseCustomInterceptor {

      private volatile boolean expectSkipIndexingFlag;

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         if (command instanceof FlagAffectedCommand) {
            boolean hasFlag = ((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.SKIP_INDEXING);
            if (expectSkipIndexingFlag && !hasFlag) {
               throw new CacheException("SKIP_INDEXING flag is expected!");
            } else if (!expectSkipIndexingFlag && hasFlag) {
               throw new CacheException("SKIP_INDEXING flag is *not* expected!");
            }
         }
         return super.handleDefault(ctx, command);
      }
   }


}
