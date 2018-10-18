package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;

public class TxReadOnlyKeyCommand<K, V, R> extends ReadOnlyKeyCommand<K, V, R> {
   public static final byte COMMAND_ID = 64;

   private List<Mutation<K, V, ?>> mutations;

   public TxReadOnlyKeyCommand() {
   }

   public TxReadOnlyKeyCommand(Object key, List<Mutation<K, V, ?>> mutations, int segment,
                               Params params, DataConversion keyDataConversion,
                               DataConversion valueDataConversion,
                               ComponentRegistry componentRegistry) {
      super(key, null, segment, params, keyDataConversion, valueDataConversion, componentRegistry);
      this.mutations = mutations;
      init(componentRegistry);
   }

   public TxReadOnlyKeyCommand(ReadOnlyKeyCommand other, List<Mutation<K, V, ?>> mutations, int segment, Params params,
                               DataConversion keyDataConversion, DataConversion valueDataConversion,
                               ComponentRegistry componentRegistry) {
      super(other.getKey(), other.f, segment, params, keyDataConversion, valueDataConversion, componentRegistry);
      this.mutations = mutations;
      init(componentRegistry);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output);
      MarshallUtil.marshallCollection(mutations, output, Mutations::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      mutations = MarshallUtil.unmarshallCollection(input, ArrayList::new, Mutations::readFrom);
   }

   @Override
   public void init(ComponentRegistry componentRegistry) {
      super.init(componentRegistry);
      // This may be called from parent's constructor when mutations are not initialized yet
      if (mutations != null) {
         for (Mutation<?, ?, ?> m : mutations) {
            m.inject(componentRegistry);
         }
      }
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("TxReadOnlyKeyCommand{");
      sb.append("key=").append(key);
      sb.append(", f=").append(f);
      sb.append(", mutations=").append(mutations);
      sb.append(", params=").append(params);
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }

   public List<Mutation<K, V, ?>> getMutations() {
      return mutations;
   }
}
