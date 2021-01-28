package org.infinispan.test.op;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;
import static org.infinispan.functional.FunctionalTestUtils.rw;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.functional.MetaParam;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;

/**
* Represents a functional write operation to test.
*
* @author Dan Berindei
* @since 12.0
*/
public enum TestFunctionalWriteOperation implements TestOperation {
   // Functional put create must return null even on retry (as opposed to non-functional)
   PUT_CREATE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, null, null, null),
   // Functional put overwrite must return the previous value (as opposed to non-functional)
   PUT_OVERWRITE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, "v0", "v0", "v0"),
   PUT_IF_ABSENT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, null, null),
   // Functional replace must return the previous value (as opposed to non-functional)
   REPLACE_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_NON_NULL, "v0", "v0", "v0"),
   REMOVE_FUNCTIONAL(ReadWriteKeyCommand.class, BackupWriteCommand.class, null, ValueMatcher.MATCH_NON_NULL, "v0", "v0", null),
   REPLACE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   REMOVE_EXACT_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, null, ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   // Functional replace
   REPLACE_META_FUNCTIONAL(ReadWriteKeyValueCommand.class, BackupWriteCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, "v0", true, true)
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

   TestFunctionalWriteOperation(Class<? extends VisitableCommand> commandClass,
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

   @Override
   public void insertPreviousValue(AdvancedCache<Object, Object> cache, Object key) {
      switch (this) {
         case REPLACE_META_FUNCTIONAL:
            FunctionalMap.WriteOnlyMap<Object, Object> woMap = FunctionalTestUtils.wo(cache);
            FunctionalTestUtils.await(woMap.eval(key, wo -> {
               wo.set("v0", new MetaParam.MetaEntryVersion(new NumericVersion(1)));
            }));
            break;
         default:
            if (previousValue != null) {
               cache.withFlags(Flag.IGNORE_RETURN_VALUES).put(key, previousValue);
            }
      }
   }

   @Override
   public Object perform(AdvancedCache<Object, Object> cache, Object key) {
      AdvancedCache<Object, Object> functionalCache = FunctionalAdvancedCache.create(cache);
      switch (this) {
         case PUT_CREATE_FUNCTIONAL:
            return functionalCache.put(key, value);
         case PUT_OVERWRITE_FUNCTIONAL:
            return functionalCache.put(key, value);
         case PUT_IF_ABSENT_FUNCTIONAL:
            return functionalCache.putIfAbsent(key, value);
         case REPLACE_FUNCTIONAL:
            return functionalCache.replace(key, value);
         case REPLACE_EXACT_FUNCTIONAL:
            return functionalCache.replace(key, previousValue, value);
         case REMOVE_FUNCTIONAL:
            return functionalCache.remove(key);
         case REMOVE_EXACT_FUNCTIONAL:
            return functionalCache.remove(key, previousValue);
         case REPLACE_META_FUNCTIONAL:
            return FunctionalTestUtils.await(rw(cache).eval(key, "v1", (v, rw) -> {
               return rw.findMetaParam(MetaParam.MetaEntryVersion.class)
                        .filter(ver -> ver.get().compareTo(new NumericVersion(1)) == EQUAL)
                        .map(ver -> {
                           rw.set(v, new MetaParam.MetaEntryVersion(new NumericVersion(2)));
                           return true;
                        }).orElse(false);
            }));
         default:
            throw new IllegalArgumentException("Unsupported operation: " + this);
      }
   }

   @Override
   public CompletionStage<?> performAsync(AdvancedCache<Object, Object> cache, Object key) {
      AdvancedCache<Object, Object> functionalCache = FunctionalAdvancedCache.create(cache);
      switch (this) {
         case PUT_CREATE_FUNCTIONAL:
         case PUT_OVERWRITE_FUNCTIONAL:
            return functionalCache.putAsync(key, value);
         case PUT_IF_ABSENT_FUNCTIONAL:
            return functionalCache.putIfAbsentAsync(key, value);
         case REPLACE_FUNCTIONAL:
            return functionalCache.replaceAsync(key, value);
         case REPLACE_EXACT_FUNCTIONAL:
            return functionalCache.replaceAsync(key, previousValue, value);
         case REMOVE_FUNCTIONAL:
            return functionalCache.removeAsync(key);
         case REMOVE_EXACT_FUNCTIONAL:
            return functionalCache.removeAsync(key, previousValue);
         case REPLACE_META_FUNCTIONAL:
            return rw(cache).eval(key, "v1", (v, rw) -> {
               return rw.findMetaParam(MetaParam.MetaEntryVersion.class)
                        .filter(ver -> ver.get().compareTo(new NumericVersion(1)) == EQUAL)
                        .map(ver -> {
                           rw.set(v, new MetaParam.MetaEntryVersion(new NumericVersion(2)));
                           return true;
                        }).orElse(false);
            });
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
