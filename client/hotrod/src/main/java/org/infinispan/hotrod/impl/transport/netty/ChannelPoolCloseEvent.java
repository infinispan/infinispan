package org.infinispan.hotrod.impl.transport.netty;

public class ChannelPoolCloseEvent {
   public static final ChannelPoolCloseEvent INSTANCE = new ChannelPoolCloseEvent();

   private ChannelPoolCloseEvent() {}
}
