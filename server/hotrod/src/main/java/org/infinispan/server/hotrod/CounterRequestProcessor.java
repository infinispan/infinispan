package org.infinispan.server.hotrod;

import static org.infinispan.commons.util.concurrent.CompletableFutures.extractException;

import java.util.Collection;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.exception.CounterNotFoundException;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.server.hotrod.counter.listener.ClientCounterManagerNotificationManager;
import org.infinispan.server.hotrod.counter.listener.ListenerOperationStatus;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

class CounterRequestProcessor extends BaseRequestProcessor {

   private final ClientCounterManagerNotificationManager notificationManager;
   private final EmbeddedCounterManager counterManager;

   CounterRequestProcessor(Channel channel, EmbeddedCounterManager counterManager, Executor executor, HotRodServer server) {
      super(channel, executor, server);
      this.counterManager = counterManager;
      notificationManager = server.getClientCounterNotificationManager();
   }

   private EmbeddedCounterManager counterManager(HotRodHeader header) {
      header.cacheName = CounterModuleLifecycle.COUNTER_CACHE_NAME;
      return counterManager;
   }

   void removeCounterListener(HotRodHeader header, Subject subject, String counterName, byte[] listenerId) {
      executor.execute(() -> removeCounterListenerInternal(header, counterName, listenerId));
   }

   private void removeCounterListenerInternal(HotRodHeader header, String counterName, byte[] listenerId) {
      try {
         writeResponse(header, createResponseFrom(header, notificationManager
               .removeCounterListener(listenerId, counterName)));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void addCounterListener(HotRodHeader header, Subject subject, String counterName, byte[] listenerId) {
      executor.execute(() -> addCounterListenerInternal(header, counterName, listenerId));
   }

   private void addCounterListenerInternal(HotRodHeader header, String counterName, byte[] listenerId) {
      try {
         writeResponse(header, createResponseFrom(header, notificationManager
               .addCounterListener(listenerId, header.getVersion(), counterName, channel, header.encoder())));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void getCounterNames(HotRodHeader header, Subject subject) {
      Collection<String> counterNames = counterManager(header).getCounterNames();
      writeResponse(header, header.encoder().counterNamesResponse(header, server, channel, counterNames));
   }

   void counterRemove(HotRodHeader header, Subject subject, String counterName) {
      counterManager(header).removeAsync(counterName, true)
            .whenComplete((___, throwable) -> voidResultHandler(header, throwable));
   }

   void counterCompareAndSwap(HotRodHeader header, Subject subject, String counterName, long expect, long update) {
      counterManager(header).getStrongCounterAsync(counterName)
            .thenCompose(strongCounter -> strongCounter.compareAndSwap(expect, update))
            .whenComplete((returnValue, throwable) -> longResultHandler(header, returnValue, throwable));
   }

   void counterGet(HotRodHeader header, Subject subject, String counterName) {
      counterManager(header).getOrCreateAsync(counterName)
            .thenCompose(InternalCounterAdmin::value)
            .whenComplete((value, throwable) -> longResultHandler(header, value, throwable));
   }

   void counterReset(HotRodHeader header, Subject subject, String counterName) {
      counterManager(header).getOrCreateAsync(counterName)
            .thenCompose(InternalCounterAdmin::reset)
            .whenComplete((unused, throwable) -> voidResultHandler(header, throwable));
   }

   void counterAddAndGet(HotRodHeader header, Subject subject, String counterName, long value) {
      counterManager(header).getOrCreateAsync(counterName)
            .thenAccept(counter -> {
               if (counter.isWeakCounter()) {
                  counter.asWeakCounter().add(value).whenComplete((___, t) -> longResultHandler(header, 0L, t));
               } else {
                  counter.asStrongCounter().addAndGet(value).whenComplete((rv, t) -> longResultHandler(header, rv, t));
               }
            })
            .exceptionally(throwable -> {
               checkCounterThrowable(header, throwable);
               return null;
            });
   }

   void counterSet(HotRodHeader header, Subject subject, String counterName, long value) {
      counterManager(header).getStrongCounterAsync(counterName)
            .thenCompose(strongCounter -> strongCounter.getAndSet(value))
            .whenComplete((returnValue, throwable) -> longResultHandler(header, returnValue, throwable));
   }

   void getCounterConfiguration(HotRodHeader header, Subject subject, String counterName) {
      counterManager(header).getConfigurationAsync(counterName)
            .whenComplete((configuration, throwable) -> handleGetCounterConfiguration(header, configuration, throwable));
   }

   private void handleGetCounterConfiguration(HotRodHeader header, CounterConfiguration configuration, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         ByteBuf response = configuration == null ? missingCounterResponse(header) :
               header.encoder().counterConfigurationResponse(header, server, channel, configuration);
         writeResponse(header, response);
      }
   }

   void isCounterDefined(HotRodHeader header, Subject subject, String counterName) {
      counterManager(header).isDefinedAsync(counterName).whenComplete((value, throwable) -> booleanResultHandler(header, value, throwable));
   }

   void createCounter(HotRodHeader header, Subject subject, String counterName, CounterConfiguration configuration) {
      counterManager(header).defineCounterAsync(counterName, configuration)
            .whenComplete((value, throwable) -> booleanResultHandler(header, value, throwable));
   }

   private ByteBuf createResponseFrom(HotRodHeader header, ListenerOperationStatus status) {
      switch (status) {
         case OK:
            return header.encoder().emptyResponse(header, server, channel, OperationStatus.OperationNotExecuted);
         case OK_AND_CHANNEL_IN_USE:
            return header.encoder().emptyResponse(header, server, channel, OperationStatus.Success);
         case COUNTER_NOT_FOUND:
            return missingCounterResponse(header);
         default:
            throw new IllegalStateException();
      }
   }

   private void checkCounterThrowable(HotRodHeader header, Throwable throwable) {
      Throwable cause = extractException(throwable);
      if (cause instanceof CounterOutOfBoundsException) {
         writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.NotExecutedWithPrevious));
      } else if (cause instanceof CounterNotFoundException) {
         writeResponse(header, missingCounterResponse(header));
      } else {
         writeException(header, cause);
      }
   }

   private ByteBuf missingCounterResponse(HotRodHeader header) {
      return header.encoder().emptyResponse(header, server, channel, OperationStatus.KeyDoesNotExist);
   }

   private void booleanResultHandler(HotRodHeader header, Boolean value, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         OperationStatus status = value ? OperationStatus.Success : OperationStatus.OperationNotExecuted;
         writeResponse(header, header.encoder().emptyResponse(header, server, channel, status));
      }
   }

   private void longResultHandler(HotRodHeader header, Long value, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         writeResponse(header, header.encoder().longResponse(header, server, channel, value));
      }
   }

   private void voidResultHandler(HotRodHeader header, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.Success));
      }
   }
}
