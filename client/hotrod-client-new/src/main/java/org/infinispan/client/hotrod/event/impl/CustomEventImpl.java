package org.infinispan.client.hotrod.event.impl;

import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.commons.util.Util;

public class CustomEventImpl<T> extends AbstractClientEvent implements ClientCacheEntryCustomEvent<T> {
   private final T data;
   private final boolean retried;
   private final Type type;

   public CustomEventImpl(byte[] listenerId, T data, boolean retried, Type type) {
      super(listenerId);
      this.data = data;
      this.retried = retried;
      this.type = type;
   }

   @Override
   public T getEventData() {
      return data;
   }

   @Override
   public boolean isCommandRetried() {
      return retried;
   }

   @Override
   public Type getType() {
      return type;
   }

   @Override
   public String toString() {
      return "CustomEventImpl(" + "eventData=" + Util.toStr(data) + ", eventType=" + type + ")";
   }
}
