package org.infinispan.client.hotrod.event.impl;

import org.infinispan.client.hotrod.event.ClientEvent;

public abstract class AbstractClientEvent implements ClientEvent {
   private final byte[] listenerId;

   protected AbstractClientEvent(byte[] listenerId) {
      this.listenerId = listenerId;
   }

   public byte[] getListenerId() {
      return listenerId;
   }
}
