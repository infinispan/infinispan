package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.container.versioning.NumericVersion;

public class NumericVersionExternalizer extends AbstractMigratorExternalizer<NumericVersion> {

   public NumericVersionExternalizer() {
      super(NumericVersion.class, Ids.NUMERIC_VERSION);
   }

   @Override
   public NumericVersion readObject(ObjectInput input) throws IOException {
      return new NumericVersion(input.readLong());
   }
}
