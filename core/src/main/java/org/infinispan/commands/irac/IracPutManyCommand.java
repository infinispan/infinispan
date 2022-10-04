package org.infinispan.commands.irac;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
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
 * @since 14.0
 */
public class IracPutManyCommand extends IracUpdateKeyCommand<IntSet> {

   private static final Log log = LogFactory.getLog(IracPutManyCommand.class);

   public static final byte COMMAND_ID = 48;

   private static final byte WRITE = 0;
   private static final byte REMOVE = 1;
   private static final byte EXPIRE = 2;

   private List<Update> updateList;

   @SuppressWarnings("unused")
   public IracPutManyCommand() {
      super(COMMAND_ID, null);
   }

   public IracPutManyCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracPutManyCommand(ByteString cacheName, int maxCapacity) {
      super(COMMAND_ID, cacheName);
      updateList = new ArrayList<>(maxCapacity);
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      marshallCollection(updateList, output, IracPutManyCommand::writeUpdateTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      updateList = unmarshallCollection(input, ArrayList::new, IracPutManyCommand::readUpdateFrom);
   }

   @Override
   public String toString() {
      return "IracPutManyCommand{" +
            "cacheName=" + cacheName +
            ", originSite='" + originSite + '\'' +
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

   private static void writeUpdateTo(ObjectOutput output, Update update) throws IOException {
      output.writeByte(update.getType());
      update.writeTo(output);
   }

   private static Update readUpdateFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      switch (input.readByte()) {
         case WRITE:
            return new Write(input.readObject(), IracMetadata.readFrom(input), input.readObject(), (Metadata) input.readObject());
         case REMOVE:
            return new Remove(input.readObject(), IracMetadata.readFrom(input));
         case EXPIRE:
            return new Expire(input.readObject(), IracMetadata.readFrom(input));
      }
      throw new IllegalStateException();
   }

   public boolean isEmpty() {
      return updateList.isEmpty();
   }

   private interface Update {
      byte getType();

      CompletionStage<Void> execute(BackupReceiver backupReceiver);

      void writeTo(ObjectOutput output) throws IOException;
   }

   private static class Remove implements Update {

      final Object key;
      final IracMetadata iracMetadata;

      private Remove(Object key, IracMetadata iracMetadata) {
         this.key = key;
         this.iracMetadata = iracMetadata;
      }

      @Override
      public byte getType() {
         return REMOVE;
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver backupReceiver) {
         return backupReceiver.removeKey(key, iracMetadata, false);
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         output.writeObject(key);
         IracMetadata.writeTo(output, iracMetadata);
      }

      @Override
      public String toString() {
         return "Remove{" +
               "key=" + Util.toStr(key) +
               ", iracMetadata=" + iracMetadata +
               '}';
      }
   }

   private static final class Expire extends Remove {

      private Expire(Object key, IracMetadata tombstone) {
         super(key, tombstone);
      }

      @Override
      public byte getType() {
         return EXPIRE;
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

   private static final class Write extends Remove {

      private final Object value;
      private final Metadata metadata;

      private Write(Object key, IracMetadata metadata, Object value, Metadata metadata1) {
         super(key, metadata);
         this.value = value;
         this.metadata = metadata1;
      }

      @Override
      public byte getType() {
         return WRITE;
      }

      @Override
      public CompletionStage<Void> execute(BackupReceiver backupReceiver) {
         return backupReceiver.putKeyValue(key, value, metadata, iracMetadata);
      }

      @Override
      public void writeTo(ObjectOutput output) throws IOException {
         super.writeTo(output);
         output.writeObject(value);
         output.writeObject(metadata);
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
