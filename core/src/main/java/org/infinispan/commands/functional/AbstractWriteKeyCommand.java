package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;

public abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand implements FunctionalCommand<K, V> {

   Params params;
   ValueMatcher valueMatcher;
   boolean successful = true;
   DataConversion keyDataConversion;
   DataConversion valueDataConversion;

   public AbstractWriteKeyCommand(Object key, ValueMatcher valueMatcher, int segment,
                                  CommandInvocationId id, Params params,
                                  DataConversion keyDataConversion,
                                  DataConversion valueDataConversion) {
      super(key, segment, params.toFlagsBitSet(), id);
      this.valueMatcher = valueMatcher;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
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

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            " {key=" + toStr(key) +
            ", flags=" + printFlags() +
            ", commandInvocationId=" + commandInvocationId +
            ", params=" + params +
            ", valueMatcher=" + valueMatcher +
            ", successful=" + successful +
            "}";
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   abstract public void init(ComponentRegistry componentRegistry);
}
