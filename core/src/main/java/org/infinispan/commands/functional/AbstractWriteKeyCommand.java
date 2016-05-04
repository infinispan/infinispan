package org.infinispan.commands.functional;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.functional.impl.Params;

public abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand implements ParamsCommand {

   Params params;
   ValueMatcher valueMatcher;
   boolean successful = true;

   public AbstractWriteKeyCommand(K key, ValueMatcher valueMatcher,
         CommandInvocationId id, Params params) {
      super(key, null, id);
      this.valueMatcher = valueMatcher;
      this.params = params;
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

   @Override
   public Params getParams() {
      return params;
   }
}
