package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.functional.impl.Params;

public abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand implements FunctionalCommand<K, V> {

   Params params;
   boolean successful = true;

   public AbstractWriteKeyCommand(K key, CommandInvocationId id, Params params) {
      super(key, EnumUtil.EMPTY_BIT_SET, id, null);
      this.params = params;
   }

   public AbstractWriteKeyCommand() {
      // No-op
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
            ", successful=" + successful +
            "}";
   }

}
