package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Response.createEmptyResponse;
import static org.infinispan.util.concurrent.CompletableFutures.extractException;

import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.server.hotrod.counter.CounterAddDecodeContext;
import org.infinispan.server.hotrod.counter.CounterCompareAndSetDecodeContext;
import org.infinispan.server.hotrod.counter.CounterCreateDecodeContext;
import org.infinispan.server.hotrod.counter.CounterListenerDecodeContext;
import org.infinispan.server.hotrod.counter.listener.ClientCounterManagerNotificationManager;
import org.infinispan.server.hotrod.counter.listener.ListenerOperationStatus;
import org.infinispan.server.hotrod.counter.response.CounterConfigurationResponse;
import org.infinispan.server.hotrod.counter.response.CounterNamesResponse;
import org.infinispan.server.hotrod.counter.response.CounterValueResponse;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.Channel;

class CounterRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(CounterRequestProcessor.class, Log.class);

   private final ClientCounterManagerNotificationManager notificationManager;
   private final EmbeddedCounterManager counterManager;

   private final BiConsumer<CacheDecodeContext, StrongCounter> handleStrongGet = this::handleGetStrong;
   private final BiConsumer<CacheDecodeContext, WeakCounter> handleWeakGet = this::handleGetWeak;
   private final BiConsumer<CacheDecodeContext, StrongCounter> handleStrongReset = this::handleResetStrong;
   private final BiConsumer<CacheDecodeContext, WeakCounter> handleWeakReset = this::handleResetWeak;

   CounterRequestProcessor(Channel channel, EmbeddedCounterManager counterManager, Executor executor, HotRodServer server) {
      super(channel, executor);
      this.counterManager = counterManager;
      notificationManager = server.getClientCounterNotificationManager();
   }

   private EmbeddedCounterManager counterManager(CacheDecodeContext cdc) {
      cdc.header.cacheName = CounterModuleLifecycle.COUNTER_CACHE_NAME;
      return counterManager;
   }

   void removeCounterListener(CacheDecodeContext cdc) {
      executor.execute(() -> removeCounterListenerInternal(cdc));
   }

   private void removeCounterListenerInternal(CacheDecodeContext cdc) {
      try {
         CounterListenerDecodeContext opCtx = cdc.operationContext();
         writeResponse(createResponseFrom(cdc, notificationManager
               .removeCounterListener(opCtx.getListenerId(), opCtx.getCounterName())));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void addCounterListener(CacheDecodeContext cdc) {
      executor.execute(() -> addCounterListenerInternal(cdc));
   }

   private void addCounterListenerInternal(CacheDecodeContext cdc) {
      try {
         CounterListenerDecodeContext opCtx = cdc.operationContext();
         writeResponse(createResponseFrom(cdc, notificationManager
               .addCounterListener(opCtx.getListenerId(), cdc.header.getVersion(), opCtx.getCounterName(), channel)));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void getCounterNames(CacheDecodeContext cdc) {
      writeResponse(new CounterNamesResponse(cdc.header, counterManager(cdc).getCounterNames()));
   }

   void counterRemove(CacheDecodeContext cdc) {
      executor.execute(() -> counterRemoveInternal(cdc));
   }

   private void counterRemoveInternal(CacheDecodeContext cdc) {
      try {
         String counterName = cdc.operationContext();
         counterManager(cdc).remove(counterName);
         writeResponse(createEmptyResponse(cdc.header, OperationStatus.Success));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void counterCompareAndSwap(CacheDecodeContext cdc) {
      CounterCompareAndSetDecodeContext decodeContext = cdc.operationContext();
      final long expect = decodeContext.getExpected();
      final long update = decodeContext.getUpdate();
      final String name = decodeContext.getCounterName();

      applyCounter(cdc, name,
            (cdcx, counter) -> counter.compareAndSwap(expect, update).whenComplete((value, throwable) -> longResultHandler(cdcx, value, throwable)),
            (cdcx, counter) -> writeException(cdcx, log.invalidWeakCounter(name))
      );
   }

   void counterGet(CacheDecodeContext cdc) {
      applyCounter(cdc, cdc.operationContext(), handleStrongGet, handleWeakGet);
   }

   private void handleGetStrong(CacheDecodeContext cdc, StrongCounter counter) {
      counter.getValue().whenComplete((value, throwable) -> longResultHandler(cdc, value, throwable));
   }

   private void handleGetWeak(CacheDecodeContext cdc, WeakCounter counter) {
      longResultHandler(cdc, counter.getValue(), null);
   }

   void counterReset(CacheDecodeContext cdc) {
      applyCounter(cdc, cdc.operationContext(), handleStrongReset, handleWeakReset);
   }

   private void handleResetStrong(CacheDecodeContext cdc, StrongCounter counter) {
      counter.reset().whenComplete((value, throwable) -> voidResultHandler(cdc, throwable));
   }

   private void handleResetWeak(CacheDecodeContext cdc, WeakCounter counter) {
      counter.reset().whenComplete((value, throwable) -> voidResultHandler(cdc, throwable));
   }

   void counterAddAndGet(CacheDecodeContext cdc) {
      CounterAddDecodeContext decodeContext = cdc.operationContext();
      final long value = decodeContext.getValue();
      applyCounter(cdc, decodeContext.getCounterName(),
            (cdcx, counter) -> counter.addAndGet(value).whenComplete((value1, throwable) -> longResultHandler(cdcx, value1, throwable)),
            (cdcx, counter) -> counter.add(value).whenComplete((value2, throwable1) -> longResultHandler(cdcx, 0L, throwable1)));
   }

   void getCounterConfiguration(CacheDecodeContext cdc) {
      counterManager(cdc).getConfigurationAsync(cdc.operationContext())
            .whenComplete((configuration, throwable) -> handleGetCounterConfiguration(cdc, configuration, throwable));
   }

   private void handleGetCounterConfiguration(CacheDecodeContext cdc, CounterConfiguration configuration, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(cdc, throwable);
      } else {
         Response response = configuration == null ? missingCounterResponse(cdc) : new CounterConfigurationResponse(cdc.header, configuration);
         writeResponse(response);
      }
   }

   void isCounterDefined(CacheDecodeContext cdc) {
      counterManager(cdc).isDefinedAsync(cdc.operationContext()).whenComplete((value, throwable) -> booleanResultHandler(cdc, value, throwable));
   }

   void createCounter(CacheDecodeContext cdc) {
      CounterCreateDecodeContext decodeContext = cdc.operationContext();
      counterManager(cdc).defineCounterAsync(decodeContext.getCounterName(), decodeContext.getConfiguration())
            .whenComplete((value, throwable) -> booleanResultHandler(cdc, value, throwable));
   }

   private void applyCounter(CacheDecodeContext cdc, String counterName,
                             BiConsumer<CacheDecodeContext, StrongCounter> applyStrong,
                             BiConsumer<CacheDecodeContext, WeakCounter> applyWeak) {
      EmbeddedCounterManager counterManager = counterManager(cdc);
      CounterConfiguration config = counterManager.getConfiguration(counterName);
      if (config == null) {
         writeResponse(missingCounterResponse(cdc));
         return;
      }
      switch (config.type()) {
         case UNBOUNDED_STRONG:
         case BOUNDED_STRONG:
            applyStrong.accept(cdc, counterManager.getStrongCounter(counterName));
            break;
         case WEAK:
            applyWeak.accept(cdc, counterManager.getWeakCounter(counterName));
            break;
      }
   }

   private Response createResponseFrom(CacheDecodeContext cdc, ListenerOperationStatus status) {
      switch (status) {
         case OK:
            return createEmptyResponse(cdc.header, OperationStatus.OperationNotExecuted);
         case OK_AND_CHANNEL_IN_USE:
            return createEmptyResponse(cdc.header, OperationStatus.Success);
         case COUNTER_NOT_FOUND:
            return missingCounterResponse(cdc);
         default:
            throw new IllegalStateException();
      }
   }

   private void checkCounterThrowable(CacheDecodeContext cdc, Throwable throwable) {
      Throwable cause = extractException(throwable);
      if (cause instanceof CounterOutOfBoundsException) {
         writeResponse(createEmptyResponse(cdc.header, OperationStatus.NotExecutedWithPrevious));
      } else {
         writeException(cdc, cause);
      }
   }

   private Response missingCounterResponse(CacheDecodeContext cdc) {
      return createEmptyResponse(cdc.header, OperationStatus.KeyDoesNotExist);
   }

   private void booleanResultHandler(CacheDecodeContext cdc, Boolean value, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(cdc, throwable);
      } else {
         Response response = createEmptyResponse(cdc.header,
               value ? OperationStatus.Success : OperationStatus.OperationNotExecuted);
         writeResponse(response);
      }
   }

   private void longResultHandler(CacheDecodeContext cdc, Long value, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(cdc, throwable);
      } else {
         writeResponse(new CounterValueResponse(cdc.header, value));
      }
   }

   private void voidResultHandler(CacheDecodeContext cdc, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(cdc, throwable);
      } else {
         writeResponse(createEmptyResponse(cdc.header, OperationStatus.Success));
      }
   }
}
