package org.infinispan.xsite.commands.remote;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupReceiver;

/**
 * A multi-key cross-site requests.
 * <p>
 * This command is used by asynchronous cross-site replication to send multiple keys batched to remote sites. The keys
 * in this command can include updates, removal or expirations.
 * <p>
 * The element order in {@code updateList} is important because the reply will be a {@link IntSet} with the position of
 * the failed keys.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_PUT_MANY_REQUEST)
public class IracPutManyRequest extends IracUpdateKeyRequest<IntSet> {

   private static final Log log = LogFactory.getLog(IracPutManyRequest.class);

   final List<Update> updateList;

   public IracPutManyRequest(ByteString cacheName, int maxCapacity) {
      super(cacheName);
      updateList = new ArrayList<>(maxCapacity);
   }

   @ProtoFactory
   IracPutManyRequest(ByteString cacheName, MarshallableList<Update> updateList) {
      super(cacheName);
      this.updateList = MarshallableList.unwrap(updateList);
   }

   @ProtoField(2)
   MarshallableList<Update> getUpdateList() {
      return MarshallableList.create(updateList);
   }

   public CompletionStage<IntSet> executeOperation(BackupReceiver receiver) {
      IntSet rsp = IntSets.concurrentSet(updateList.size());
      AggregateCompletionStage<IntSet> stage = CompletionStages.aggregateCompletionStage(rsp);
      for (int i = 0; i < updateList.size(); ++i) {
         int keyIndex = i;
         Update update = updateList.get(i);
         stage.dependsOn(update.execute(receiver).exceptionally(throwable -> {
            if (log.isTraceEnabled()) {
               log.tracef(throwable, "[IRAC] Received exception while applying %s", update);
            }
            rsp.set(keyIndex);
            return null;
         }));
      }
      return stage.freeze();
   }

   @Override
   public String toString() {
      return "IracPutManyRequest{" +
            "cacheName=" + cacheName +
            ", updateList=" + Util.toStr(updateList) +
            '}';
   }

   public void addUpdate(Object key, Object value, Metadata metadata, IracMetadata iracMetadata) {
      updateList.add(new Write(key, iracMetadata, value, metadata));
   }

   public void addRemove(Object key, IracMetadata tombstone) {
      updateList.add(new Remove(key, tombstone));
   }

   public void addExpire(Object key, IracMetadata tombstone) {
      updateList.add(new Expire(key, tombstone));
   }

   public boolean isEmpty() {
      return updateList.isEmpty();
   }

   private interface Update {
      CompletionStage<Void> execute(BackupReceiver backupReceiver);
   }


   // We must explicitly provide a ProtoName to prevent name clashing with classes in MarshallableFunction
   // This has no impact on the wire format as we're already using ProtoTypeID for both classes
   @ProtoName("IracPutManyRequestRemove")
   @ProtoTypeId(ProtoStreamTypeIds.IRAC_PUT_MANY_REQUEST_REMOVE)
   public static class Remove implements Update {

      final Object key;

      @ProtoField(1)
      MarshallableObject<Object> getKey() {
         return MarshallableObject.create(key);
      }

      @ProtoField(2)
      final IracMetadata iracMetadata;

      @ProtoFactory
      Remove(MarshallableObject<Object> key, IracMetadata iracMetadata) {
         this(MarshallableObject.unwrap(key),  iracMetadata);
      }

      Remove(Object key, IracMetadata iracMetadata) {
         this.key = key;
         this.iracMetadata = iracMetadata;
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver backupReceiver) {
         return backupReceiver.removeKey(key, iracMetadata, false);
      }

      @Override
      public String toString() {
         return "Remove{" +
               "key=" + Util.toStr(key) +
               ", iracMetadata=" + iracMetadata +
               '}';
      }
   }

   @ProtoName("IracPutManyRequestExpire")
   @ProtoTypeId(ProtoStreamTypeIds.IRAC_PUT_MANY_REQUEST_EXPIRE)
   public static final class Expire extends Remove {

      @ProtoFactory
      Expire(MarshallableObject<Object> key, IracMetadata iracMetadata) {
         this(MarshallableObject.unwrap(key),  iracMetadata);
      }

      private Expire(Object key, IracMetadata tombstone) {
         super(key, tombstone);
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver backupReceiver) {
         return backupReceiver.removeKey(key, iracMetadata, true);
      }

      @Override
      public String toString() {
         return "Expire{" +
               "key=" + Util.toStr(key) +
               ", iracMetadata=" + iracMetadata +
               '}';
      }
   }

   @ProtoName("IracPutManyRequestWrite")
   @ProtoTypeId(ProtoStreamTypeIds.IRAC_PUT_MANY_REQUEST_WRITE)
   public static final class Write extends Remove {

      private final Object value;
      private final Metadata metadata;

      private Write(Object key, IracMetadata iracMetadata, Object value, Metadata metadata) {
         super(key, iracMetadata);
         this.value = value;
         this.metadata = metadata;
      }

      @ProtoFactory
      Write(MarshallableObject<Object> key, IracMetadata iracMetadata, MarshallableObject<Object> value, MarshallableObject<Metadata> metadata) {
         this (MarshallableObject.unwrap(key), iracMetadata, MarshallableObject.unwrap(value), MarshallableObject.unwrap(metadata));
      }

      @ProtoField(3)
      MarshallableObject<Object> getValue() {
         return MarshallableObject.create(value);
      }

      @ProtoField(4)
      MarshallableObject<Metadata> getMetadata() {
         return MarshallableObject.create(metadata);
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver backupReceiver) {
         return backupReceiver.putKeyValue(key, value, metadata, iracMetadata);
      }

      @Override
      public String toString() {
         return "Write{" +
               "key=" + Util.toStr(key) +
               ", value=" + Util.toStr(value) +
               ", iracMetadata=" + iracMetadata +
               ", metadata=" + metadata +
               '}';
      }
   }
}
