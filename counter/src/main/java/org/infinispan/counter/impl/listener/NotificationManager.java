package org.infinispan.counter.impl.listener;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.logging.Log;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A notification manager for {@link CounterListener}.
 * <p>
 * It allows to register (and remove) {@link CounterListener} and it triggers the notification via {@link
 * #notify(CounterEvent)}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class NotificationManager {

   private static final Log log = LogFactory.getLog(NotificationManager.class, Log.class);

   private final List<CounterListenerResponse> listenerList;

   public NotificationManager() {
      listenerList = new CopyOnWriteArrayList<>();
   }

   /**
    * It adds a {@link CounterListener}.
    * <p>
    * Duplicated value are <b>not</b> added.
    *
    * @param listener The listener to add.
    * @param <T>      The type of the listener.
    * @return The {@link Handle} that allows the listener to be removed.
    */
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      CounterListenerResponse<T> wrapper = new CounterListenerResponse<>(Objects.requireNonNull(listener), listenerList);
      for (Handle<?> clc : listenerList) {
         if (wrapper.equals(clc)) {
            //noinspection unchecked
            return (Handle<T>) clc;
         }
      }
      listenerList.add(wrapper);
      return wrapper;
   }

   /**
    * It notifies all the registered listeners with the {@link CounterEvent}.
    *
    * @param event The {@link CounterEvent} to trigger.
    */
   public void notify(CounterEvent event) {
      listenerList.forEach(l -> l.onUpdate(event));
   }

   private static class CounterListenerResponse<T extends CounterListener> implements Handle<T>, CounterListener {

      private final T listener;
      private final List<CounterListenerResponse> list;

      private CounterListenerResponse(T listener, List<CounterListenerResponse> list) {
         this.listener = listener;
         this.list = list;
      }

      @Override
      public T getCounterListener() {
         return listener;
      }

      @Override
      public void remove() {
         list.remove(this);
      }

      @Override
      public void onUpdate(CounterEvent event) {
         try {
            listener.onUpdate(event);
         } catch (Throwable t) {
            log.warnf(t, "Exception while invoking listener %s", listener);
         }
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CounterListenerResponse<?> that = (CounterListenerResponse<?>) o;

         return listener.equals(that.listener);
      }

      @Override
      public int hashCode() {
         return listener.hashCode();
      }
   }

}
