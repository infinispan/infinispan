package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
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

import java.util.Properties;

import static org.infinispan.client.hotrod.Flag.FORCE_RETURN_VALUE;
import static org.infinispan.client.hotrod.Flag.SKIP_CACHE_LOAD;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests if the {@link org.infinispan.client.hotrod.Flag#SKIP_CACHE_LOAD} flag is received on HotRod server.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(testName = "client.hotrod.SkipCacheLoadFlagTest", groups = "functional")
@CleanupAfterTest
public class SkipCacheLoadFlagTest extends SingleCacheManagerTest {

   private static final String KEY = "key";
   private static final String VALUE = "value";
   private FlagCheckCommandInterceptor commandInterceptor;
   private RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotRodServer;

   public void testPut() {
      //PutRequest
      performTest(RequestType.PUT);
   }

   public void testReplace() {
      //ReplaceRequest
      performTest(RequestType.REPLACE);

   }

   public void testPutIfAbsent() {
      //PutIfAbsentRequest
      performTest(RequestType.PUT_IF_ABSENT);
   }

   public void testReplaceIfUnmodified() {
      //ReplaceIfUnmodifiedRequest
      performTest(RequestType.REPLACE_IF_UNMODIFIED);
   }

   public void testGet() {
      //GetRequest
      performTest(RequestType.GET);
   }

   public void testGetWithVersion() {
      //GetWithVersionRequest
      performTest(RequestType.GET_WITH_VERSION);

   }

   public void testGetWithMetadata() {
      //GetWithMetadataRequest
      performTest(RequestType.GET_WITH_METADATA);
   }

   public void testRemove() {
      //RemoveRequest
      performTest(RequestType.REMOVE);
   }

   public void testRemoveIfUnmodified() {
      //RemoveIfUnmodifiedRequest
      performTest(RequestType.REMOVE_IF_UNMODIFIED);
   }

   public void testContainsKey() {
      //ContainsKeyRequest
      performTest(RequestType.CONTAINS);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotRodClientConf = new Properties();
      hotRodClientConf.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(hotRodClientConf);
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
      commandInterceptor.expectSkipLoadFlag = false;
      type.execute(remoteCache);

      commandInterceptor.expectSkipLoadFlag = RequestType.expectsFlag(type);
      type.execute(remoteCache.withFlags(SKIP_CACHE_LOAD));
      type.execute(remoteCache.withFlags(SKIP_CACHE_LOAD, FORCE_RETURN_VALUE));
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
         commandInterceptor.expectSkipLoadFlag = false;
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
      };

      private static boolean expectsFlag(RequestType type) {
         return type != PUT_IF_ABSENT && type != REMOVE_IF_UNMODIFIED && type != REPLACE && type != REPLACE_IF_UNMODIFIED;
      }

      abstract void execute(RemoteCache<String, String> cache);
   }

   private static class FlagCheckCommandInterceptor extends BaseCustomInterceptor {

      private volatile boolean expectSkipLoadFlag;

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         if (command instanceof LocalFlagAffectedCommand) {
            boolean hasFlag = ((LocalFlagAffectedCommand) command).hasFlag(Flag.SKIP_CACHE_LOAD);
            if (expectSkipLoadFlag && !hasFlag) {
               throw new CacheException("SKIP_CACHE_LOAD flag is expected!");
            } else if (!expectSkipLoadFlag && hasFlag) {
               throw new CacheException("SKIP_CACHE_LOAD flag is *not* expected!");
            }
         }
         return super.handleDefault(ctx, command);
      }
   }


}
