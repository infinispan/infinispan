package org.infinispan.client.hotrod.impl.transport.netty;

public class ChannelPoolCloseEvent {
   public static ChannelPoolCloseEvent INSTANCE = new ChannelPoolCloseEvent();

   private ChannelPoolCloseEvent() {}
}
