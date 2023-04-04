package org.infinispan.util.logging.events.impl;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.transaction.Transaction;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.impl.AbstractListenerImpl;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.annotation.impl.Logged;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLoggerNotifier;

@Scope(Scopes.GLOBAL)
public class EventLoggerNotifierImpl extends AbstractListenerImpl<EventLog, ListenerInvocation<EventLog>> implements EventLoggerNotifier {

   private static final Log log = LogFactory.getLog(EventLoggerNotifierImpl.class);
   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<>(1);
   private final List<ListenerInvocation<EventLog>> listeners = new CopyOnWriteArrayList<>();

   static {
      allowedListeners.put(Logged.class, EventLog.class);
   }

   public EventLoggerNotifierImpl() {
      listenersMap.put(Logged.class, listeners);
   }

   private class DefaultBuilder extends AbstractInvocationBuilder {

      @Override
      public ListenerInvocation<EventLog> build() {
         return new ListenerInvocationImpl<>(target, method, sync, classLoader, subject);
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations(Listener l) {
      return allowedListeners;
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      validateAndAddListenerInvocations(listener, new DefaultBuilder());
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      removeListenerFromMaps(listener);
      return CompletableFutures.completedNull();
   }

   @Override
   protected Transaction suspendIfNeeded() {
      return null;
   }

   @Override
   protected void resumeIfNeeded(Transaction transaction) {
      // no-op
   }

   @Override
   protected void handleException(Throwable t) {
      log.failedInvokingEventLoggerListener(t);
   }

   @Override
   public CompletionStage<Void> notifyEventLogged(EventLog log) {
      if (!listeners.isEmpty()) {
         return invokeListeners(log, listeners);
      }
      return CompletableFutures.completedNull();
   }
}
