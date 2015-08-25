package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;

abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand {

   ValueMatcher valueMatcher;
   boolean successful = true;

   public AbstractWriteKeyCommand(K key, ValueMatcher valueMatcher, CommandInvocationId id) {
      super(key, null, id);
      this.valueMatcher = valueMatcher;
   }

   public AbstractWriteKeyCommand() {
      // No-op
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

}
