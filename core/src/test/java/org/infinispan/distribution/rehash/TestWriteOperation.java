package org.infinispan.distribution.rehash;

import java.util.Collections;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.write.BackupMultiKeyWriteRpcCommand;
import org.infinispan.commands.write.BackupWriteRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;

/**
* Represents a write operation to test.
*
* @author Dan Berindei
* @since 6.0
*/
public enum TestWriteOperation {
   PUT_CREATE(PutKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, null, null, "v1"),
   PUT_OVERWRITE(PutKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, "v0", "v0", "v1"),
   PUT_IF_ABSENT(PutKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, null, null),
   REPLACE(ReplaceCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_NON_NULL, "v0", "v0", "v1"),
   REPLACE_EXACT(ReplaceCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   REMOVE(RemoveCommand.class, BackupWriteRpcCommand.class, null, ValueMatcher.MATCH_NON_NULL, "v0", "v0", null),
   REMOVE_EXACT(RemoveCommand.class, BackupWriteRpcCommand.class, null, ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   PUT_MAP_CREATE(PutMapCommand.class, BackupMultiKeyWriteRpcCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, false, false),

   // Functional put create must return null even on retry (as opposed to non-functional)
   PUT_CREATE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, null, null, null),
   // Functional put overwrite must return the previous value (as opposed to non-functional)
   PUT_OVERWRITE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, "v0", "v0", "v0"),
   PUT_IF_ABSENT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, null, null),
   // Functional replace must return the previous value (as opposed to non-functional)
   REPLACE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_NON_NULL, "v0", "v0", "v0"),
   REMOVE_FUNCTIONAL(ReadWriteKeyCommand.class, BackupWriteRpcCommand.class, null, ValueMatcher.MATCH_NON_NULL, "v0", "v0", null),
   REPLACE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   REMOVE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, null, ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   // Functional replace
   REPLACE_META_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteRpcCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, true, true)
   ;

   private final Class<? extends VisitableCommand> commandClass;
   private Class<? extends ReplicableCommand> backupCommandClass;
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

   public Class<? extends VisitableCommand> getCommandClass() {
      return commandClass;
   }

   public Class<? extends ReplicableCommand> getBackupCommandClass() {
      return backupCommandClass;
   }

   public Object getValue() {
      return value;
   }

   public Object getPreviousValue() {
      return previousValue;
   }

   public Object getReturnValue() {
      return returnValue;
   }

   public Object perform(AdvancedCache<Object, Object> cache, Object key) {
      switch (this) {
         case PUT_CREATE:
         case PUT_OVERWRITE:
         case PUT_CREATE_FUNCTIONAL:
         case PUT_OVERWRITE_FUNCTIONAL:
            return cache.put(key, value);
         case PUT_IF_ABSENT:
         case PUT_IF_ABSENT_FUNCTIONAL:
            return cache.putIfAbsent(key, value);
         case REPLACE:
         case REPLACE_FUNCTIONAL:
            return cache.replace(key, value);
         case REPLACE_EXACT:
         case REPLACE_EXACT_FUNCTIONAL:
            return cache.replace(key, previousValue, value);
         case REMOVE:
         case REMOVE_FUNCTIONAL:
            return cache.remove(key);
         case REMOVE_EXACT:
         case REMOVE_EXACT_FUNCTIONAL:
            return cache.remove(key, previousValue);
         case PUT_MAP_CREATE:
            cache.putAll(Collections.singletonMap(key, value));
            return null;
         default:
            throw new IllegalArgumentException("Unsupported operation: " + this);
      }
   }

   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   public Object getReturnValueWithRetry() {
      return returnValueWithRetry;
   }
}
