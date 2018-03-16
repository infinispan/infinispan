package org.infinispan.server.hotrod;

import static org.infinispan.util.concurrent.CompletableFutures.extractException;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import javax.security.auth.Subject;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.server.hotrod.counter.listener.ClientCounterManagerNotificationManager;
import org.infinispan.server.hotrod.counter.listener.ListenerOperationStatus;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

class CounterRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(CounterRequestProcessor.class, Log.class);

   private final ClientCounterManagerNotificationManager notificationManager;
   private final EmbeddedCounterManager counterManager;

   private final BiConsumer<HotRodHeader, StrongCounter> handleStrongGet = this::handleGetStrong;
   private final BiConsumer<HotRodHeader, WeakCounter> handleWeakGet = this::handleGetWeak;
   private final BiConsumer<HotRodHeader, StrongCounter> handleStrongReset = this::handleResetStrong;
   private final BiConsumer<HotRodHeader, WeakCounter> handleWeakReset = this::handleResetWeak;

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
      writeResponse(header, header.encoder().counterNamesResponse(header, server, channel.alloc(), counterNames));
   }

   void counterRemove(HotRodHeader header, Subject subject, String counterName) {
      executor.execute(() -> counterRemoveInternal(header, counterName));
   }

   private void counterRemoveInternal(HotRodHeader header, String counterName) {
      try {
         counterManager(header).remove(counterName);
         writeSuccess(header);
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void counterCompareAndSwap(HotRodHeader header, Subject subject, String counterName, long expect, long update) {
      applyCounter(header, counterName,
            (h, counter) -> counter.compareAndSwap(expect, update).whenComplete((value, throwable) -> longResultHandler(h, value, throwable)),
            (h, counter) -> writeException(h, log.invalidWeakCounter(counterName))
      );
   }

   void counterGet(HotRodHeader header, Subject subject, String counterName) {
      applyCounter(header, counterName, handleStrongGet, handleWeakGet);
   }

   private void handleGetStrong(HotRodHeader header, StrongCounter counter) {
      counter.getValue().whenComplete((value, throwable) -> longResultHandler(header, value, throwable));
   }

   private void handleGetWeak(HotRodHeader header, WeakCounter counter) {
      longResultHandler(header, counter.getValue(), null);
   }

   void counterReset(HotRodHeader header, Subject subject, String counterName) {
      applyCounter(header, counterName, handleStrongReset, handleWeakReset);
   }

   private void handleResetStrong(HotRodHeader header, StrongCounter counter) {
      counter.reset().whenComplete((value, throwable) -> voidResultHandler(header, throwable));
   }

   private void handleResetWeak(HotRodHeader header, WeakCounter counter) {
      counter.reset().whenComplete((value, throwable) -> voidResultHandler(header, throwable));
   }

   void counterAddAndGet(HotRodHeader header, Subject subject, String counterName, long value) {
      applyCounter(header, counterName,
            (h, counter) -> counter.addAndGet(value).whenComplete((value1, throwable) -> longResultHandler(h, value1, throwable)),
            (h, counter) -> counter.add(value).whenComplete((value2, throwable1) -> longResultHandler(h, 0L, throwable1)));
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
               header.encoder().counterConfigurationResponse(header, server, channel.alloc(), configuration);
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

   private void applyCounter(HotRodHeader header, String counterName,
                             BiConsumer<HotRodHeader, StrongCounter> applyStrong,
                             BiConsumer<HotRodHeader, WeakCounter> applyWeak) {
      EmbeddedCounterManager counterManager = counterManager(header);
      CounterConfiguration config = counterManager.getConfiguration(counterName);
      if (config == null) {
         writeResponse(header, missingCounterResponse(header));
         return;
      }
      switch (config.type()) {
         case UNBOUNDED_STRONG:
         case BOUNDED_STRONG:
            applyStrong.accept(header, counterManager.getStrongCounter(counterName));
            break;
         case WEAK:
            applyWeak.accept(header, counterManager.getWeakCounter(counterName));
            break;
      }
   }

   private ByteBuf createResponseFrom(HotRodHeader header, ListenerOperationStatus status) {
      switch (status) {
         case OK:
            return header.encoder().emptyResponse(header, server, channel.alloc(), OperationStatus.OperationNotExecuted);
         case OK_AND_CHANNEL_IN_USE:
            return header.encoder().emptyResponse(header, server, channel.alloc(), OperationStatus.Success);
         case COUNTER_NOT_FOUND:
            return missingCounterResponse(header);
         default:
            throw new IllegalStateException();
      }
   }

   private void checkCounterThrowable(HotRodHeader header, Throwable throwable) {
      Throwable cause = extractException(throwable);
      if (cause instanceof CounterOutOfBoundsException) {
         writeResponse(header, header.encoder().emptyResponse(header, server, channel.alloc(), OperationStatus.NotExecutedWithPrevious));
      } else {
         writeException(header, cause);
      }
   }

   private ByteBuf missingCounterResponse(HotRodHeader header) {
      return header.encoder().emptyResponse(header, server, channel.alloc(), OperationStatus.KeyDoesNotExist);
   }

   private void booleanResultHandler(HotRodHeader header, Boolean value, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         OperationStatus status = value ? OperationStatus.Success : OperationStatus.OperationNotExecuted;
         writeResponse(header, header.encoder().emptyResponse(header, server, channel.alloc(), status));
      }
   }

   private void longResultHandler(HotRodHeader header, Long value, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         writeResponse(header, header.encoder().longResponse(header, server, channel.alloc(), value));
      }
   }

   private void voidResultHandler(HotRodHeader header, Throwable throwable) {
      if (throwable != null) {
         checkCounterThrowable(header, throwable);
      } else {
         writeResponse(header, header.encoder().emptyResponse(header, server, channel.alloc(), OperationStatus.Success));
      }
   }
}
