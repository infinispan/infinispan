package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests the behaviour of the client upon a socket timeout exception and any invocation after that.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.SocketTimeoutErrorTest")
public class SocketTimeoutErrorTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(
            new TimeoutInducingInterceptor()).after(EntryWrappingInterceptor.class);
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(builder));
   }

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.socketTimeout(2000);
      builder.maxRetries(0);
      return new RemoteCacheManager(builder.build());
   }

   public void testErrorWhileDoingPut(Method m) {
      RemoteCache<String, Integer> cache = remoteCacheManager.getCache();

      cache.put(k(m), 1);
      assertEquals(1, cache.get(k(m)).intValue());
      Exceptions.expectException(TransportException.class, SocketTimeoutException.class, () -> cache.put("FailFailFail", 2));
      cache.put("dos", 2);
      assertEquals(2, cache.get("dos").intValue());

      TestingUtil.extractInterceptorChain(this.cache)
                 .findInterceptorWithClass(TimeoutInducingInterceptor.class)
                 .stopBlocking();
   }

   public static class TimeoutInducingInterceptor extends BaseCustomAsyncInterceptor {
      public final CompletableFuture<Void> delay = new CompletableFuture<>();

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (unmarshall(command.getKey()).equals("FailFailFail")) {
            return asyncValue(delay);
         }

         return super.visitPutKeyValueCommand(ctx, command);
      }

      private String unmarshall(Object key) throws Exception {
         return (String) new ProtoStreamMarshaller().objectFromByteBuffer(((WrappedByteArray) key).getBytes());
      }

      private void stopBlocking() {
         delay.complete(null);
      }
   }
}
