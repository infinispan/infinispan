package org.infinispan.commands.functional;

import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.functional.impl.ListenerNotifier;

abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand {

   ListenerNotifier<K, V> notifier;
   ValueMatcher valueMatcher;
   boolean successful = true;

   public AbstractWriteKeyCommand(K key, SerializeWith ann) {
      super(key, null);
      this.valueMatcher = ann != null
         ? ValueMatcher.valueOf(ann.valueMatcher().toString())
         : ValueMatcher.MATCH_ALWAYS;
   }

   public void init(ListenerNotifier<K, V> notifier) {
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
