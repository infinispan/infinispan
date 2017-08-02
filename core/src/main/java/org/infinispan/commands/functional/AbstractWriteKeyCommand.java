package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.cache.impl.CacheEncoders;
import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;

public abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand implements FunctionalCommand<K, V> {

   Params params;
   ValueMatcher valueMatcher;
   boolean successful = true;
   EncodingClasses encodingClasses;
   CacheEncoders cacheEncoders = CacheEncoders.EMPTY;

   public AbstractWriteKeyCommand(K key, ValueMatcher valueMatcher,
                                  CommandInvocationId id, Params params) {
      super(key, EnumUtil.EMPTY_BIT_SET, id);
      this.valueMatcher = valueMatcher;
      this.params = params;
      this.setFlagsBitSet(params.toFlagsBitSet());
   }

   public AbstractWriteKeyCommand(K key, ValueMatcher valueMatcher,
                                  CommandInvocationId id, Params params,
                                  EncodingClasses encodingClasses) {
      super(key, EnumUtil.EMPTY_BIT_SET, id);
      this.valueMatcher = valueMatcher;
      this.params = params;
      this.setFlagsBitSet(params.toFlagsBitSet());
      this.encodingClasses = encodingClasses;
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
   public EncodingClasses getEncodingClasses() {
      return encodingClasses;
   }

   abstract public void init(ComponentRegistry componentRegistry);
}
