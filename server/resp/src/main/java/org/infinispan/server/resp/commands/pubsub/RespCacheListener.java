package org.infinispan.server.resp.commands.pubsub;

public interface RespCacheListener {

   byte[] subscribedChannel();

   byte[] pattern();
}
