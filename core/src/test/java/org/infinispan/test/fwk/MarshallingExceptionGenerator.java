package org.infinispan.test.fwk;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.test.TestException;

/**
 * Class that throws an exception on serialization or deserialization.
 *
 * @author Dan Berindei
 * @since 12.1
 */
public class MarshallingExceptionGenerator implements Externalizable {
   private int countDownToMarshallingException;
   private int countDownToUnmarshallingException;

   public static MarshallingExceptionGenerator failOnSerialization(int count) {
      return new MarshallingExceptionGenerator(count, -1);
   }

   public static MarshallingExceptionGenerator failOnDeserialization(int count) {
      return new MarshallingExceptionGenerator(-1, count);
   }

   public MarshallingExceptionGenerator(int countDownToMarshallingException, int countDownToUnmarshallingException) {
      this.countDownToMarshallingException = countDownToMarshallingException;
      this.countDownToUnmarshallingException = countDownToUnmarshallingException;
   }

   public MarshallingExceptionGenerator() {
      this(0, 0);
   }

   @Override
   public String toString() {
      return "MarshallingExceptionGenerator{" +
             "countDownToMarshallingException=" + countDownToMarshallingException +
             ", countDownToUnmarshallingException=" + countDownToUnmarshallingException +
             '}';
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {
      if (countDownToMarshallingException == 0) {
         throw new TestException();
      }
      out.writeInt(countDownToMarshallingException - 1);
      out.writeInt(countDownToUnmarshallingException - 1);
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException {
      this.countDownToMarshallingException = in.readInt();
      this.countDownToUnmarshallingException = in.readInt();
      if (countDownToUnmarshallingException == -1) {
         throw new TestException();
      }
   }
}
