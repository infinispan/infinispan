package org.infinispan.commands.functional;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;

public abstract class AbstractWriteKeyCommand<K, V> extends AbstractDataWriteCommand implements FunctionalCommand<K, V> {

   Params params;
   boolean successful = true;
   DataConversion keyDataConversion;
   DataConversion valueDataConversion;

   public AbstractWriteKeyCommand(Object key,
                                  CommandInvocationId id, Params params,
                                  DataConversion keyDataConversion,
                                  DataConversion valueDataConversion,
                                  InvocationManager invocationManager) {
      super(key, EnumUtil.EMPTY_BIT_SET, id, invocationManager);
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
      this.setFlagsBitSet(params.toFlagsBitSet());
   }

   public AbstractWriteKeyCommand() {
      // No-op
   }

   public void init(InvocationManager invocationManager, ComponentRegistry componentRegistry) {
      this.invocationManager = invocationManager;
      init(componentRegistry);
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

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   protected abstract void init(ComponentRegistry componentRegistry);
}
