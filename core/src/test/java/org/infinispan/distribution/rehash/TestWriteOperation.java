package org.infinispan.distribution.rehash;

import java.util.Collections;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
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
   PUT_CREATE("v1", null, null, PutKeyValueCommand.class),
   PUT_OVERWRITE("v1", "v0", "v0",  PutKeyValueCommand.class),
   PUT_IF_ABSENT("v1", null, null,  PutKeyValueCommand.class),
   REPLACE("v1", "v0", "v0", ReplaceCommand.class, PutKeyValueCommand.class),
   REPLACE_EXACT("v1", "v0", true,  ReplaceCommand.class, PutKeyValueCommand.class),
   REMOVE(null, "v0", "v0", RemoveCommand.class),
   REMOVE_EXACT(null, "v0", true, RemoveCommand.class),
   PUT_MAP_CREATE("v1", null, false, PutMapCommand.class),

   // Functional put create must return null even on retry (as opposed to non-functional)
   PUT_CREATE_FUNCTIONAL("v1", null, null, ReadWriteKeyValueCommand.class),
   // Functional put overwrite must return the previous value (as opposed to non-functional)
   PUT_OVERWRITE_FUNCTIONAL("v1", "v0", "v0", ReadWriteKeyValueCommand.class),
   PUT_IF_ABSENT_FUNCTIONAL("v1", null, null, ReadWriteKeyValueCommand.class),
   // Functional replace must return the previous value (as opposed to non-functional)
   REPLACE_FUNCTIONAL("v1", "v0", "v0", ReadWriteKeyValueCommand.class),
   REMOVE_FUNCTIONAL(null, "v0", "v0", ReadWriteKeyCommand.class),
   REPLACE_EXACT_FUNCTIONAL("v1", "v0", true, ReadWriteKeyValueCommand.class),
   REMOVE_EXACT_FUNCTIONAL(null, "v0", true, ReadWriteKeyValueCommand.class),
   // Functional replace
   REPLACE_META_FUNCTIONAL("v1", null, true, ReadWriteKeyValueCommand.class)
   ;

   private final Class<? extends VisitableCommand>[] commandClasses;
   private final Object value;
   private final Object previousValue;
   private final Object returnValue;

   TestWriteOperation(Object value, Object previousValue, Object returnValue, Class<? extends VisitableCommand>... commandClasses) {
      this.commandClasses = commandClasses;
      this.value = value;
      this.previousValue = previousValue;
      this.returnValue = returnValue;
   }

   public Class<? extends VisitableCommand>[] getCommandClasses() {
      return commandClasses;
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
