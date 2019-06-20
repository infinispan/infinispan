package org.infinispan.test.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoField;

public class Key implements Externalizable {
   private static final long serialVersionUID = 4745232904453872125L;

   @ProtoField(number = 1)
   String value;

   public Key() {}

   public Key(String value) {
      this.value = value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      return Objects.equals(value, ((Key) o).value);
   }

   @Override
   public int hashCode() {
      return value != null ? value.hashCode() : 0;
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(value);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      value = (String) in.readObject();
   }
}
