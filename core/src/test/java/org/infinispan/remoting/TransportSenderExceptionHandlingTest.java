/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.remoting;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.jgroups.blocks.RpcDispatcher;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.lang.reflect.InvocationTargetException;
import java.util.EmptyStackException;

import static org.mockito.Mockito.*;

@Test(groups = "functional", testName = "remoting.TransportSenderExceptionHandlingTest")
public class TransportSenderExceptionHandlingTest extends MultipleCacheManagersTest {
   private final String key = "k-illyria", value = "v-illyria", value2 = "v2-illyria";

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, "replSync",
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
   }

   public void testInvokeAndExceptionWhileUnmarshalling() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      JGroupsTransport transport1 = (JGroupsTransport) TestingUtil.extractComponent(cache1, Transport.class);
      CommandAwareRpcDispatcher dispatcher1 = transport1.getCommandAwareRpcDispatcher();
      RpcDispatcher.Marshaller originalMarshaller1 = dispatcher1.getMarshaller();
      JGroupsTransport transport2 = (JGroupsTransport) TestingUtil.extractComponent(cache2, Transport.class);
      CommandAwareRpcDispatcher dispatcher2 = transport2.getCommandAwareRpcDispatcher();
      RpcDispatcher.Marshaller originalMarshaller = dispatcher2.getMarshaller();
      try {
         RpcDispatcher.Marshaller mockMarshaller1 = mock(RpcDispatcher.Marshaller.class);
         RpcDispatcher.Marshaller mockMarshaller = mock(RpcDispatcher.Marshaller.class);
         PutKeyValueCommand putCommand = new PutKeyValueCommand();
         putCommand.setKey(key);
         putCommand.setValue(value);
         SingleRpcCommand rpcCommand = new SingleRpcCommand("replSync");
         Object[] params = new Object[]{putCommand};
         rpcCommand.setParameters(SingleRpcCommand.COMMAND_ID, params);
         when(mockMarshaller1.objectToBuffer(anyObject())).thenReturn(originalMarshaller1.objectToBuffer(rpcCommand));
         when(mockMarshaller.objectFromBuffer((byte[]) anyObject(), anyInt(), anyInt())).thenThrow(new EOFException());
         dispatcher1.setRequestMarshaller(mockMarshaller1);
         dispatcher2.setRequestMarshaller(mockMarshaller);
         cache1.put(key, value);
         assert false : "Should have thrown an exception";
      } catch (RemoteException ce) {
         assert !(ce.getCause() instanceof ClassCastException) : "No way a ClassCastException must be sent back to user!";
         assert ce.getCause() instanceof CacheException;
         assert ce.getCause().getCause() instanceof EOFException;
      } finally {
         dispatcher1.setMarshaller(originalMarshaller1);
         dispatcher2.setMarshaller(originalMarshaller);
      }
   }

   @Test(expectedExceptions = ArrayStoreException.class)
   public void testThrowExceptionFromRemoteListener() throws Throwable {
      induceListenerMalfunctioning(false, FailureType.EXCEPTION_FROM_LISTENER);
   }

   @Test(expectedExceptions = NoClassDefFoundError.class)
   public void testThrowErrorFromRemoteListener() throws Throwable {
      induceListenerMalfunctioning(true, FailureType.ERROR_FROM_LISTENER);
   }

   @Test(expectedExceptions = EmptyStackException.class)
   public void testThrowExceptionFromRemoteInterceptor() throws Throwable {
      induceInterceptorMalfunctioning(FailureType.EXCEPTION_FROM_INTERCEPTOR);
   }

   @Test(expectedExceptions = ClassCircularityError.class)
   public void testThrowErrorFromRemoteInterceptor() throws Throwable {
      induceInterceptorMalfunctioning(FailureType.ERROR_FROM_INTERCEPTOR);
   }

   private void induceInterceptorMalfunctioning(FailureType failureType) throws Throwable {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      cache2.getAdvancedCache().addInterceptorAfter(
            new ErrorInducingInterceptor(), NonTransactionalLockingInterceptor.class);

      log.info("Before put.");
      try {
         cache1.put(failureType, 1);
      } catch (CacheException e) {
         Throwable cause = e.getCause();
         if (cause.getCause() == null)
            throw cause;
         else
            throw cause.getCause();
      } finally {
         cache2.getAdvancedCache().removeInterceptor(ErrorInducingInterceptor.class);
      }
   }

   private void induceListenerMalfunctioning(boolean throwError, FailureType failureType) throws Throwable {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      ErrorInducingListener listener = new ErrorInducingListener(throwError);
      cache2.addListener(listener);
      try {
         cache1.put(failureType, 1);
      } catch (RemoteException e) {
         Throwable cause = e.getCause(); // get the exception behind the remote one
         if (throwError && cause.getCause() instanceof InvocationTargetException)
            throw cause.getCause().getCause();
         else
            throw cause.getCause();
      } finally {
         cache2.removeListener(listener);
      }
   }

   @Listener
   public static class ErrorInducingListener {
      final boolean throwError;

      public ErrorInducingListener(boolean throwError) {
         this.throwError = throwError;
      }

      @CacheEntryCreated
      public void entryCreated(CacheEntryEvent event) throws Exception {
         if (event.isPre() && shouldFail(event)) {
            if (throwError)
               throw new NoClassDefFoundError("Simulated error...");
            else
               throw new ArrayStoreException("A failure...");
         }
      }

      private boolean shouldFail(CacheEntryEvent event) {
         Object key = event.getKey();
         return key == FailureType.EXCEPTION_FROM_LISTENER
               || key == FailureType.ERROR_FROM_LISTENER;
      }
   }

   static enum FailureType {
      EXCEPTION_FROM_LISTENER, ERROR_FROM_LISTENER,
      EXCEPTION_FROM_INTERCEPTOR, ERROR_FROM_INTERCEPTOR;
   }

   static class ErrorInducingInterceptor extends CommandInterceptor {
      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         Object k = command.getKey();
         if (k == FailureType.EXCEPTION_FROM_INTERCEPTOR)
            throw new EmptyStackException();
         else if (k == FailureType.ERROR_FROM_INTERCEPTOR)
            throw new ClassCircularityError();
         else
            return super.visitPutKeyValueCommand(ctx, command);
      }
   }
}
