package org.infinispan.test.data;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.protostream.annotations.ProtoField;

public class BrokenMarshallingPojo {

   private boolean failOnMarshalling = true;

   public BrokenMarshallingPojo() {}

   public BrokenMarshallingPojo(boolean failOnMarshalling) {
      this.failOnMarshalling = failOnMarshalling;
   }

   @ProtoField(number = 1, defaultValue = "true")
   public boolean getIgnored() {
      if (failOnMarshalling)
         throw new MarshallingException();
      return false;
   }

   public void setIgnored(boolean ignore) {
      if (!ignore)
         throw new MarshallingException();
   }
}
