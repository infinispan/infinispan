package org.infinispan.client.hotrod.retry;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.retry.ReceiveTransactionExceptionTest", groups = "functional")
public class ReceiveTransactionExceptionTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(3, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .lockAcquisitionTimeout(5, TimeUnit.SECONDS);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC).transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.clustering().hash().numOwners(2);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(HotRodServer server) {
      var builder = super.createHotRodClientConfigurationBuilder(server);
      builder.clientIntelligence(ClientIntelligence.TOPOLOGY_AWARE);
      builder.socketTimeout(30_000).connectionTimeout(30_000).transactionTimeout(1, TimeUnit.MINUTES);
      return builder;
   }

   @Override
   protected int maxRetries() {
      return 1;
   }

   public void testTransactionFailed() {
      RemoteCache<Integer, String> rc0 = clients.get(0).getCache();
      installOnAllManagers();

      assertThatThrownBy(() -> rc0.put(1, "value"))
            .isInstanceOf(HotRodClientException.class)
            .hasMessageContaining("Oops");
   }

   private void installOnAllManagers() {
      for (EmbeddedCacheManager manager : managers()) {
         TestingUtil.extractInterceptorChain(manager.getCache())
               .addInterceptor(new FailureInterceptor(), 2);
      }
   }

   static final class FailureInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            throw new RuntimeException("Oops");
         });
      }
   }
}
