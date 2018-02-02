package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.functional.FunctionalCommand;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;
import org.infinispan.util.ByteString;

/**
 * A base {@link BackupWriteCommand} used by {@link FunctionalCommand}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
abstract class FunctionalBackupWriteCommand extends BackupWriteCommand {

   Object function;
   Params params;
   DataConversion keyDataConversion;
   DataConversion valueDataConversion;

   ComponentRegistry componentRegistry;

   FunctionalBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   final void writeFunctionAndParams(ObjectOutput output) throws IOException {
      output.writeObject(function);
      Params.writeObject(output, params);
      DataConversion.writeTo(output, keyDataConversion);
      DataConversion.writeTo(output, valueDataConversion);
   }

   final void readFunctionAndParams(ObjectInput input) throws IOException, ClassNotFoundException {
      function = input.readObject();
      params = Params.readObject(input);
      keyDataConversion = DataConversion.readFrom(input);
      valueDataConversion = DataConversion.readFrom(input);

   }

   final void setFunctionalCommand(FunctionalCommand command) {
      this.params = command.getParams();
      this.keyDataConversion = command.getKeyDataConversion();
      this.valueDataConversion = command.getValueDataConversion();
   }
}
