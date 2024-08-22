package org.infinispan.server.resp.meta;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Metadata for `<code>INFO</code>` command.
 * <p>
 * Holds metadata about client connections. All the values are specific to a single node.
 * </p>
 *
 * @since 15.1
 * @see org.infinispan.server.resp.commands.INFO
 */
public final class ClientMetadata {

   private static final AtomicLongFieldUpdater<ClientMetadata> CONNECTED = newUpdater(ClientMetadata.class, "connectedClients");
   private static final AtomicLongFieldUpdater<ClientMetadata> BLOCKED = newUpdater(ClientMetadata.class, "blockedClients");
   private static final AtomicLongFieldUpdater<ClientMetadata> PUB_SUB = newUpdater(ClientMetadata.class, "pubSubClients");
   private static final AtomicLongFieldUpdater<ClientMetadata> WATCH = newUpdater(ClientMetadata.class, "watchingClients");
   private static final AtomicLongFieldUpdater<ClientMetadata> KEYS_BLOCKED = newUpdater(ClientMetadata.class, "blockedKeys");
   private static final AtomicLongFieldUpdater<ClientMetadata> KEYS_WATCH = newUpdater(ClientMetadata.class, "watchedKeys");

   /**
    * Number of client connections
    */
   private volatile long connectedClients = 0;

   /**
    * Number of clients pending on a blocking call (BLPOP, BRPOP, BRPOPLPUSH, BLMOVE, BZPOPMIN, BZPOPMAX)
    */
   private volatile long blockedClients = 0;

   /**
    * Number of clients in pubsub mode (SUBSCRIBE, PSUBSCRIBE, SSUBSCRIBE)
    */
   private volatile long pubSubClients = 0;

   /**
    * Number of clients in watching mode (WATCH)
    */
   private volatile long watchingClients = 0;

   /**
    * Number of blocking keys
    */
   private volatile long blockedKeys = 0;

   /**
    * Number of watched keys
    */
   private volatile long watchedKeys = 0;

   public void incrementConnectedClients() {
      CONNECTED.incrementAndGet(this);
   }

   public void decrementConnectedClients() {
      CONNECTED.decrementAndGet(this);
   }

   public void incrementBlockedClients() {
      BLOCKED.incrementAndGet(this);
   }

   public void decrementBlockedClients() {
      BLOCKED.decrementAndGet(this);
   }

   public void incrementPubSubClients() {
      PUB_SUB.incrementAndGet(this);
   }

   public void decrementPubSubClients() {
      PUB_SUB.decrementAndGet(this);
   }

   public void incrementWatchingClients() {
      WATCH.incrementAndGet(this);
   }

   public void decrementWatchingClients() {
      WATCH.decrementAndGet(this);
   }

   public long getConnectedClients() {
      return CONNECTED.get(this);
   }

   public long getBlockedClients() {
      return BLOCKED.get(this);
   }

   public long getPubSubClients() {
      return PUB_SUB.get(this);
   }

   public long getWatchingClients() {
      return WATCH.get(this);
   }

   public void recordBlockedKeys(long value) {
      KEYS_BLOCKED.addAndGet(this, value);
   }

   public void recordWatchedKeys(long value) {
      KEYS_WATCH.addAndGet(this, value);
   }

   public long getBlockedKeys() {
      return KEYS_BLOCKED.get(this);
   }

   public long getWatchedKeys() {
      return KEYS_WATCH.get(this);
   }
}
