package org.infinispan.api.client.listener;

import java.util.Arrays;
import java.util.List;

import org.infinispan.api.Experimental;
import org.infinispan.api.reactive.EntryStatus;
import org.infinispan.api.reactive.listener.KeyValueStoreListener;

@Experimental
public final class ClientKeyValueStoreListener implements KeyValueStoreListener {
   private final boolean listenUpdated;
   private final boolean listenCreated;
   private final boolean listenDeleted;

   private ClientKeyValueStoreListener(EntryStatus[] types) {
      List<EntryStatus> eventTypes = Arrays.asList(types);
      listenCreated = eventTypes.contains(EntryStatus.CREATED);
      listenUpdated = eventTypes.contains(EntryStatus.UPDATED);
      listenDeleted = eventTypes.contains(EntryStatus.DELETED);
   }

   public static KeyValueStoreListener create() {
      return new ClientKeyValueStoreListener();
   }

   public static KeyValueStoreListener create(EntryStatus... status) {
      return new ClientKeyValueStoreListener(status);
   }

   private ClientKeyValueStoreListener() {
      listenCreated = true;
      listenUpdated = true;
      listenDeleted = true;
   }

   public boolean isListenCreated() {
      return listenCreated;
   }

   public boolean isListenUpdated() {
      return listenUpdated;
   }

   public boolean isListenDeleted() {
      return listenDeleted;
   }
}
