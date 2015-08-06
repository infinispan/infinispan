package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.functional.impl.FunctionalNotifier;

abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand {

   FunctionalNotifier<K, V> notifier;
   ValueMatcher valueMatcher;
   boolean successful = true;

   public AbstractWriteKeyCommand(K key, SerializeWith ann, CommandInvocationId id) {
      super(key, null, id);
      this.valueMatcher = ann != null
         ? ValueMatcher.valueOf(ann.valueMatcher().toString())
         : ValueMatcher.MATCH_ALWAYS;
   }

   public void init(FunctionalNotifier<K, V> notifier) {
      this.notifier = notifier;
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
