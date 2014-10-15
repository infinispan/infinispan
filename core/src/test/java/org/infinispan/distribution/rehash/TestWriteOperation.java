package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
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
   PUT_CREATE(PutKeyValueCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, null, null, "v1"),
   PUT_OVERWRITE(PutKeyValueCommand.class, "v1", ValueMatcher.MATCH_ALWAYS, "v0", "v0", "v1"),
   PUT_IF_ABSENT(PutKeyValueCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, null, null, null),
   REPLACE(ReplaceCommand.class, "v1", ValueMatcher.MATCH_NON_NULL, "v0", "v0", "v1"),
   REPLACE_EXACT(ReplaceCommand.class, "v1", ValueMatcher.MATCH_EXPECTED, "v0", true, true),
   REMOVE(RemoveCommand.class, null, ValueMatcher.MATCH_NON_NULL, "v0", "v0", null),
   REMOVE_EXACT(RemoveCommand.class, null, ValueMatcher.MATCH_EXPECTED, "v0", true, true);

   private final Class<? extends VisitableCommand> commandClass;
   private final Object value;
   private final ValueMatcher valueMatcher;
   private final Object previousValue;
   private final Object returnValue;
   // When retrying a write operation, we don't always have the previous value, so we sometimes
   // return the new value instead. For "exact" conditional operations, however, we always return the same value.
   // See https://issues.jboss.org/browse/ISPN-3422
   private final Object returnValueWithRetry;

   TestWriteOperation(Class<? extends VisitableCommand> commandClass, Object value, ValueMatcher valueMatcher,
         Object previousValue, Object returnValue, Object returnValueWithRetry) {
      this.commandClass = commandClass;
      this.value = value;
      this.valueMatcher = valueMatcher;
      this.previousValue = previousValue;
      this.returnValue = returnValue;
      this.returnValueWithRetry = returnValueWithRetry;
   }

   public Class<? extends VisitableCommand> getCommandClass() {
      return commandClass;
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
