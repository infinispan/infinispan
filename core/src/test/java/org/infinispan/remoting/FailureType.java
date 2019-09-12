package org.infinispan.remoting;

import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum FailureType {
   @ProtoEnumValue(number = 1)
   EXCEPTION_FROM_LISTENER,

   @ProtoEnumValue(number = 2)
   ERROR_FROM_LISTENER,

   @ProtoEnumValue(number = 3)
   EXCEPTION_FROM_INTERCEPTOR,

   @ProtoEnumValue(number = 4)
   ERROR_FROM_INTERCEPTOR
}
