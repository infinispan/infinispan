package org.infinispan.server.hotrod.counter.listener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.ByRef;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.hotrod.VersionedEncoder;

import io.netty.channel.Channel;

/**
 * The {@link CounterManager} notification manager.
 * <p>
 * For each client, it associates a {@link ClientNotificationManager} to handle that particular client requests.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class ClientCounterManagerNotificationManager {

   private final CounterManager counterManager;
   private final Map<WrappedByteArray, ClientNotificationManager> clientManagers;

   public ClientCounterManagerNotificationManager(CounterManager counterManager) {
      this.counterManager = counterManager;
      clientManagers = new ConcurrentHashMap<>();
   }

   private static WrappedByteArray wrapId(byte[] id) {
      return new WrappedByteArray(id);
   }

   public void stop() {
      clientManagers.values().forEach(ClientNotificationManager::removeAll);
      clientManagers.clear();
   }

   public ListenerOperationStatus addCounterListener(byte[] listenerId, byte version,
                                                     String counterName, Channel channel, VersionedEncoder encoder) {
      ByRef<ListenerOperationStatus> status = new ByRef<>(ListenerOperationStatus.COUNTER_NOT_FOUND);
      clientManagers
            .compute(wrapId(listenerId), (id, manager) -> add(id, manager, version, counterName, channel, encoder, status));
      return status.get();
   }

   public ListenerOperationStatus removeCounterListener(byte[] listenerId, String counterName) {
      ByRef<ListenerOperationStatus> status = new ByRef<>(ListenerOperationStatus.COUNTER_NOT_FOUND);
      clientManagers.computeIfPresent(wrapId(listenerId), (id, manager) -> rm(manager, counterName, status));
      return status.get();
   }

   public void channelActive(Channel channel) {
      channel.eventLoop().execute(() -> clientManagers.values().forEach(manager -> manager.channelActive(channel)));
   }

   private ClientNotificationManager add(WrappedByteArray id, ClientNotificationManager manager, byte version,
                                         String counterName, Channel channel, VersionedEncoder encoder, ByRef<ListenerOperationStatus> status) {
      boolean useChannel = false;
      if (manager == null) {
         manager = new ClientNotificationManager(id.getBytes(), counterManager, channel, encoder);
         useChannel = true;
      }
      if (manager.addCounterListener(version, counterName)) {
         status.set(useChannel ? ListenerOperationStatus.OK_AND_CHANNEL_IN_USE : ListenerOperationStatus.OK);
         return manager;
      } else {
         status.set(ListenerOperationStatus.COUNTER_NOT_FOUND);
         return null;
      }
   }

   private ClientNotificationManager rm(ClientNotificationManager manager, String counterName,
         ByRef<ListenerOperationStatus> status) {
      if (counterName.isEmpty()) {
         manager.removeAll();
      } else {
         manager.removeCounterListener(counterName);
      }
      if (manager.isEmpty()) {
         status.set(ListenerOperationStatus.OK_AND_CHANNEL_IN_USE);
         return null;
      } else {
         status.set(ListenerOperationStatus.OK);
         return manager;
      }
   }
}
