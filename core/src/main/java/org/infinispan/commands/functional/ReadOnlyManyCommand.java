package org.infinispan.commands.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.impl.Params;

public class ReadOnlyManyCommand<K, V, R> extends AbstractTopologyAffectedCommand implements InitializableCommand, LocalCommand {
   public static final int COMMAND_ID = 63;

   protected Collection<?> keys;
   protected Function<ReadEntryView<K, V>, R> f;
   protected Params params;
   protected DataConversion keyDataConversion;
   protected DataConversion valueDataConversion;

   public ReadOnlyManyCommand(Collection<?> keys,
                              Function<ReadEntryView<K, V>, R> f,
                              Params params,
                              DataConversion keyDataConversion,
                              DataConversion valueDataConversion) {
      this.keys = keys;
      this.f = f;
      this.params = params;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
      this.setFlagsBitSet(params.toFlagsBitSet());
   }

   public ReadOnlyManyCommand() {
   }

   public ReadOnlyManyCommand(ReadOnlyManyCommand c) {
      this.keys = c.keys;
      this.f = c.f;
      this.params = c.params;
      this.setFlagsBitSet(c.getFlagsBitSet());
      this.keyDataConversion = c.keyDataConversion;
      this.valueDataConversion = c.valueDataConversion;
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
   }

   public Collection<?> getKeys() {
      return keys;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   public final ReadOnlyManyCommand<K, V, R> withKeys(Collection<?> keys) {
      setKeys(keys);
      return this;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(keys, output);
      output.writeObject(f);
      Params.writeObject(output, params);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.keys = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      this.f = (Function<ReadEntryView<K, V>, R>) input.readObject();
      this.params = Params.readObject(input);
      this.setFlagsBitSet(params.toFlagsBitSet());
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReadOnlyManyCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }


   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   public Params getParams() {
      return params;
   }

   public Function<ReadEntryView<K, V>, R> getFunction() {
      return f;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ReadOnlyManyCommand{");
      sb.append(", keys=").append(keys);
      sb.append(", f=").append(f.getClass().getName());
      sb.append(", keyDataConversion=").append(keyDataConversion);
      sb.append(", valueDataConversion=").append(valueDataConversion);
      sb.append('}');
      return sb.toString();
   }
}
