package org.infinispan.distribution.rehash;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;

import java.util.Collections;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.triangle.MultiEntriesFunctionalBackupWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.functional.MetaParam;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;

/**
* Represents a write operation to test.
*
* @author Dan Berindei
* @since 6.0
*/
public enum TestWriteOperation {
   PUT_CREATE(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null, false),
   PUT_OVERWRITE(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", "v0", false),
   PUT_IF_ABSENT(PutKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null, false),
   REPLACE(ReplaceCommand.class, BackupWriteCommand.class, "v1", "v0", "v0", false),
   // TODO: PutKeyValueCommand during retry?
   REPLACE_EXACT(ReplaceCommand.class, BackupWriteCommand.class, "v1", "v0", true, false),
   REMOVE(RemoveCommand.class, BackupWriteCommand.class, null, "v0", "v0", false),
   REMOVE_EXACT(RemoveCommand.class, BackupWriteCommand.class, null, "v0", true, false),
   PUT_MAP_CREATE(PutMapCommand.class, BackupWriteCommand.class, "v1", null, false, false),

   // Functional put create must return null even on retry (as opposed to non-functional)
   PUT_CREATE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null, true),
   // Functional put overwrite must return the previous value (as opposed to non-functional)
   PUT_OVERWRITE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", "v0", true),
   PUT_IF_ABSENT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", null, null, true),
   // Functional replace must return the previous value (as opposed to non-functional)
   REPLACE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", "v0", true),
   REMOVE_FUNCTIONAL(ReadWriteKeyCommand.class, BackupWriteCommand.class, null, "v0", "v0", true),
   REPLACE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", "v0", true, true),
   REMOVE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, null, "v0", true, true),
   PUT_MAP_CREATE_FUNCTIONAL(WriteOnlyManyEntriesCommand.class, MultiEntriesFunctionalBackupWriteCommand.class, "v1", null, false, true),
   // Functional replace
   REPLACE_META_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", null, true, true),

   // TODO: test WriteOnly* commands
   ;

   private final Class<? extends VisitableCommand> commandClass;
   private final Class<? extends ReplicableCommand> backupCommandClass;
   private final Object value;
   private final Object previousValue;
   private final Object returnValue;
   private final boolean functional;

   TestWriteOperation(Class<? extends VisitableCommand> commandClass,
                      Class<? extends ReplicableCommand> backupCommandClass,
                      Object value, Object previousValue, Object returnValue, boolean functional) {
      this.commandClass = commandClass;
      this.backupCommandClass = backupCommandClass;
      this.value = value;
      this.previousValue = previousValue;
      this.returnValue = returnValue;
      this.functional = functional;
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
      if (functional && !(cache instanceof FunctionalAdvancedCache)) {
         cache = FunctionalAdvancedCache.create(cache);
      }
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
         case PUT_MAP_CREATE_FUNCTIONAL:
            cache.putAll(Collections.singletonMap(key, value));
            return null;
         case REPLACE_META_FUNCTIONAL:
            return FunctionalTestUtils.await(ReadWriteMapImpl.create(FunctionalMapImpl.create(cache))
                  .eval(key, "v1", (v, rw) -> rw.findMetaParam(MetaParam.MetaEntryVersion.class)
                        .filter(ver -> ver.get().compareTo(new NumericVersion(1)) == EQUAL)
                        .map(ver -> {
                              rw.set(v, new MetaParam.MetaEntryVersion(new NumericVersion(2)));
                              return true;
                        }).orElse(false)));
         default:
            throw new IllegalArgumentException("Unsupported operation: " + this);
      }
   }
}
