package org.infinispan.test.data;

import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.TestingUtil;

public class DelayedMarshallingPojo {

   int marshallDelay;
   int unmarshallDely;

   DelayedMarshallingPojo() {}

   public DelayedMarshallingPojo(int marshallDelay, int unmarshallDely) {
      this.marshallDelay = marshallDelay;
      this.unmarshallDely = unmarshallDely;
   }

   @ProtoField(number = 1, defaultValue = "false")
   boolean getIgnored() {
      if (marshallDelay > 0)
         TestingUtil.sleepThread(marshallDelay);
      return false;
   }

   // Should only be called by protostream when unmarshalling
   void setIgnored(boolean ignored) {
      if (unmarshallDely > 0)
         TestingUtil.sleepThread(unmarshallDely);
   }
}
