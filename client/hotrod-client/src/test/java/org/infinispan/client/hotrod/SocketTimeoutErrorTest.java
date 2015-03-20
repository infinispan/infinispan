package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.SocketTimeoutException;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests the behaviour of the client upon a socket timeout exception
 * and any invocation after that.
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
      builder.workerThreads(6); // TODO: Remove workerThreads configuration when ISPN-5083 implemented
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
         new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.socketTimeout(2000);
      builder.maxRetries(0);
      return new RemoteCacheManager(builder.build());
   }

   public void testErrorWhileDoingPut(Method m) throws Exception {
      RemoteCache<String, Integer> cache = remoteCacheManager.getCache();

      cache.put(k(m), 1);
      assertEquals(1, cache.get(k(m)).intValue());

      try {
         cache.put("FailFailFail", 2);
         Assert.fail("No exception was thrown.");
      } catch (HotRodClientException e) {
         // ignore
         assert e.getCause() instanceof SocketTimeoutException;
      }

      cache.put("dos", 2);
      assertEquals(2, cache.get("dos").intValue());
   }

   public static class TimeoutInducingInterceptor extends CommandInterceptor {

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (unmarshall(command.getKey()).equals("FailFailFail")) {
            Thread.sleep(6000);
            return null;
         }

         return super.visitPutKeyValueCommand(ctx, command);
      }

      private String unmarshall(Object key) throws Exception {
         Marshaller marshaller = new JBossMarshaller();
         return (String) marshaller.objectFromByteBuffer((byte[]) key);
      }
   }

}