package org.infinispan.distribution.rehash;

import java.util.Collections;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;

/**
* Represents a write operation to test.
*
* @author Dan Berindei
* @since 6.0
*/
public enum TestWriteOperation {
   PUT_CREATE(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null),
   PUT_OVERWRITE(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", "v0"),
   PUT_IF_ABSENT(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null),
   REPLACE(ReplaceCommand.class, BackupWriteCommand.class, "v1", "v0", "v0"),
   // TODO: PutKeyValueCommand during retry?
   REPLACE_EXACT(ReplaceCommand.class, BackupWriteCommand.class, "v1", "v0", true),
   REMOVE(RemoveCommand.class, BackupWriteCommand.class, null, "v0", "v0"),
   REMOVE_EXACT(RemoveCommand.class, BackupWriteCommand.class, null, "v0", true),
   PUT_MAP_CREATE(PutMapCommand.class, BackupWriteCommand.class, "v1", null, false),

   // Functional put create must return null even on retry (as opposed to non-functional)
   PUT_CREATE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null),
   // Functional put overwrite must return the previous value (as opposed to non-functional)
   PUT_OVERWRITE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", "v0"),
   PUT_IF_ABSENT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null),
   // Functional replace must return the previous value (as opposed to non-functional)
   REPLACE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", "v0"),
   REMOVE_FUNCTIONAL(ReadWriteKeyCommand.class, BackupWriteCommand.class, null, "v0", "v0"),
   REPLACE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", true),
   REMOVE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, null, "v0", true),
   // Functional replace
   REPLACE_META_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", null, true),

   // TODO: test WriteOnly* commands
   ;

   private final Class<? extends VisitableCommand> commandClass;
   private final Class<? extends ReplicableCommand> backupCommandClass;
   private final Object value;
   private final Object previousValue;
   private final Object returnValue;

   TestWriteOperation(Class<? extends VisitableCommand> commandClass,
                      Class<? extends ReplicableCommand> backupCommandClass,
                      Object value, Object previousValue, Object returnValue) {
      this.commandClass = commandClass;
      this.backupCommandClass = backupCommandClass;
      this.value = value;
      this.previousValue = previousValue;
      this.returnValue = returnValue;
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
}
