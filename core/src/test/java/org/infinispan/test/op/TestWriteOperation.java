package org.infinispan.test.op;

import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.context.Flag;

/**
* Represents a write operation to test.
*
* @author Dan Berindei
* @since 6.0
*/
public enum TestWriteOperation implements TestOperation {
   PUT_CREATE(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, null, null, "v1"),
   PUT_OVERWRITE(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, "v0", "v0", "v1"),
   PUT_IF_ABSENT(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, null, null),
   REPLACE(ReplaceCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_NON_NULL, "v0", "v0", "v1"),
   REPLACE_EXACT(ReplaceCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   REMOVE(RemoveCommand.class, BackupWriteCommand.class, null, ValueMatcher.MATCH_NON_NULL, "v0", "v0", null),
   REMOVE_EXACT(RemoveCommand.class, BackupWriteCommand.class, null, ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   PUT_MAP_CREATE(PutMapCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, null, null),
   // TODO Add TestWriteOperation enum values for compute/computeIfAbsent/computeIfPresent/merge
   ;

   private final Class<? extends VisitableCommand> commandClass;
   private final Class<? extends ReplicableCommand> backupCommandClass;
   private final Object value;
   private final ValueMatcher valueMatcher;
   private final Object previousValue;
   private final Object returnValue;
   // When retrying a write operation, we don't always have the previous value, so we sometimes
   // return the new value instead. For "exact" conditional operations, however, we always return the same value.
   // See https://issues.jboss.org/browse/ISPN-3422
   private final Object returnValueWithRetry;

   TestWriteOperation(Class<? extends VisitableCommand> commandClass,
                      Class<? extends ReplicableCommand> backupCommandClass,
                      Object value, ValueMatcher valueMatcher,
                      Object previousValue, Object returnValue, Object returnValueWithRetry) {
      this.commandClass = commandClass;
      this.backupCommandClass = backupCommandClass;
      this.value = value;
      this.valueMatcher = valueMatcher;
      this.previousValue = previousValue;
      this.returnValue = returnValue;
      this.returnValueWithRetry = returnValueWithRetry;
   }

   @Override
   public Class<? extends VisitableCommand> getCommandClass() {
      return commandClass;
   }

   @Override
   public Class<? extends ReplicableCommand> getBackupCommandClass() {
      return backupCommandClass;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public Object getPreviousValue() {
      return previousValue;
   }

   @Override
   public Object getReturnValue() {
      return returnValue;
   }

   @Override
   public void insertPreviousValue(AdvancedCache<Object, Object> cache, Object key) {
      if (previousValue != null) {
         cache.withFlags(Flag.IGNORE_RETURN_VALUES).put(key, previousValue);
      }
   }

   @Override
   public Object perform(AdvancedCache<Object, Object> cache, Object key) {
      switch (this) {
         case PUT_CREATE:
         case PUT_OVERWRITE:
            return cache.put(key, value);
         case PUT_IF_ABSENT:
            return cache.putIfAbsent(key, value);
         case REPLACE:
            return cache.replace(key, value);
         case REPLACE_EXACT:
            return cache.replace(key, previousValue, value);
         case REMOVE:
            return cache.remove(key);
         case REMOVE_EXACT:
            return cache.remove(key, previousValue);
         case PUT_MAP_CREATE:
            cache.putAll(Collections.singletonMap(key, value));
            return null;
         default:
            throw new IllegalArgumentException("Unsupported operation: " + this);
      }
   }

   @Override
   public CompletionStage<?> performAsync(AdvancedCache<Object, Object> cache, Object key) {
      switch (this) {
         case PUT_CREATE:
         case PUT_OVERWRITE:
            return cache.putAsync(key, value);
         case PUT_IF_ABSENT:
            return cache.putIfAbsentAsync(key, value);
         case REPLACE:
            return cache.replaceAsync(key, value);
         case REPLACE_EXACT:
            return cache.replaceAsync(key, previousValue, value);
         case REMOVE:
            return cache.removeAsync(key);
         case REMOVE_EXACT:
            return cache.removeAsync(key, previousValue);
         case PUT_MAP_CREATE:
            return cache.putAllAsync(Collections.singletonMap(key, value));
         default:
            throw new IllegalArgumentException("Unsupported operation: " + this);
      }
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @Override
   public Object getReturnValueWithRetry() {
      return returnValueWithRetry;
   }
}
