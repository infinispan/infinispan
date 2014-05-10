package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;

/**
* Represents a write operation to test.
*
* @author Dan Berindei
* @since 6.0
*/
public enum TestWriteOperation {
   PUT_CREATE(PutKeyValueCommand.class, "v1", null, null),
   PUT_OVERWRITE(PutKeyValueCommand.class, "v1", "v0", "v0"),
   PUT_IF_ABSENT(PutKeyValueCommand.class, "v1", null, null),
   REPLACE(ReplaceCommand.class, "v1", "v0", "v0"),
   REPLACE_EXACT(ReplaceCommand.class, "v1", "v0", true),
   REMOVE(RemoveCommand.class, null, "v0", "v0"),
   REMOVE_EXACT(RemoveCommand.class, null, "v0", true);

   private final Class<? extends VisitableCommand> commandClass;
   private final Object value;
   private final Object previousValue;
   private final Object returnValue;

   TestWriteOperation(Class<? extends VisitableCommand> commandClass, Object value, Object previousValue,
                      Object returnValue) {
      this.commandClass = commandClass;
      this.value = value;
      this.previousValue = previousValue;
      this.returnValue = returnValue;
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
}
