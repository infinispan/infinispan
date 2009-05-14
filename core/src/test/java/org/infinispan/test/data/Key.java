package org.infinispan.test.data;

import org.infinispan.util.concurrent.ReclosableLatch;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.TimeUnit;

public class Key implements Externalizable {

   String value;
   ReclosableLatch latch = new ReclosableLatch(false);
   boolean lockable;

   public Key() {
   }

   public Key(String value, boolean lockable) {
      this.value = value;
      this.lockable = lockable;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Key k1 = (Key) o;

      if (value != null ? !value.equals(k1.value) : k1.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return value != null ? value.hashCode() : 0;
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(value);
      if (lockable) {
         try {
            if (!latch.await(60, TimeUnit.SECONDS)) throw new RuntimeException("Cannot serialize!!");
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         latch.close();
      }
   }

   public void allowSerialization() {
      if (lockable) latch.open();
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      value = (String) in.readObject();
   }
}
