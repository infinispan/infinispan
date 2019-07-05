package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertTrue;

import java.util.Properties;

import org.infinispan.client.hotrod.impl.operations.RemoveIfUnmodifiedOperation;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 * @since 9.4
 */
@Test(testName = "client.hotrod.RemoteCacheFlagPropagationTest", groups = "functional")
public class RemoteCacheFlagPropagationTest extends SingleCacheManagerTest {

   private static final String KEY = "key";
   private static final String VALUE = "value";
   private RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotRodServer;

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

   public void testFlagPropagation() {
      cache.getAdvancedCache().addInterceptor(new BaseCustomInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
            if (command instanceof FlagAffectedCommand) {
               if (command instanceof GetCacheEntryCommand) {
                  assertTrue(hasOnlyTheFlag((FlagAffectedCommand)command));
               } else if (command instanceof RemoveIfUnmodifiedOperation) {
                  assertTrue(hasOnlyTheFlag((FlagAffectedCommand)command));
               }
            }
            return super.handleDefault(ctx, command);
         }
         private boolean hasOnlyTheFlag(FlagAffectedCommand flagCommand) {
            return flagCommand.getFlags().size() == 1 && flagCommand.hasAnyFlag(FlagBitSets.SKIP_LISTENER_NOTIFICATION);
         }
      }, 1);
      // it should be always a cache call that has another call inside
      remoteCache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(KEY, VALUE);
   }
}
