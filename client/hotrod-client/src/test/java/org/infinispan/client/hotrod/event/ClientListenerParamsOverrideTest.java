package org.infinispan.client.hotrod.event;

import io.netty.buffer.ByteBuf;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec28;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.util.Deque;
import java.util.LinkedList;

import static org.infinispan.test.TestingUtil.replaceField;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerParamsOverrideTest")
public class ClientListenerParamsOverrideTest extends SingleHotRodServerTest {

   private TestCodec testCodec;

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      final RemoteCacheManager remote = super.getRemoteCacheManager();
      final RemoteCacheImpl<?, ?> cache = (RemoteCacheImpl<?, ?>) remote.getCache();
      final OperationsFactory opsFactory = cache.getOperationsFactory();
      testCodec = new TestCodec();
      replaceField(testCodec, "codec", opsFactory, OperationsFactory.class);
      return remote;
   }

   public void testClientListenerAnnotation() {
      final RemoteCache<?, ?> cache = remoteCacheManager.getCache();
      try {
         cache.addClientListener(new AnnotatedListener());
      } catch (HotRodClientException e) {
         // Expected since there are no factories
         final ClientListener listener = testCodec.clientListeners.pop();
         assertEquals("a", listener.filterFactoryName());
         assertEquals("b", listener.converterFactoryName());
         assertTrue(listener.useRawData());
         assertTrue(listener.includeCurrentState());
      }
   }

   public void testClientListenerParameter() {
      final RemoteCache<?, ?> cache = remoteCacheManager.getCache();
      try {
         ClientListener listenerParams = new ClientListener() {
            @Override
            public Class<? extends Annotation> annotationType() {
               return ClientListener.class;
            }

            @Override
            public String filterFactoryName() {
               return "c";
            }

            @Override
            public String converterFactoryName() {
               return "d";
            }

            @Override
            public boolean useRawData() {
               return true;
            }

            @Override
            public boolean includeCurrentState() {
               return true;
            }
         };

         cache.addClientListener(
            new ParameterListener(), null, null, listenerParams);
      } catch (HotRodClientException e) {
         // Expected since there are no factories
         final ClientListener listener = testCodec.clientListeners.pop();
         assertEquals("c", listener.filterFactoryName());
         assertEquals("d", listener.converterFactoryName());
         assertTrue(listener.useRawData());
         assertTrue(listener.includeCurrentState());
      }
   }

   @ClientListener(
      filterFactoryName = "a"
      , converterFactoryName = "b"
      , useRawData = true
      , includeCurrentState = true
   )
   private static final class AnnotatedListener {
   }

   private static final class ParameterListener {
   }

   private static final class TestCodec extends Codec28 {

      Deque<ClientListener> clientListeners = new LinkedList<>();

      @Override
      public void writeClientListenerParams(ByteBuf buf, ClientListener clientListener, byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
         clientListeners.push(clientListener);
         super.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      }

   }

}
