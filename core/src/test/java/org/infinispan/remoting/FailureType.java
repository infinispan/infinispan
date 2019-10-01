package org.infinispan.remoting;

import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum FailureType {
   @ProtoEnumValue(number = 0)
   EXCEPTION_FROM_LISTENER,

   @ProtoEnumValue(number = 1)
   ERROR_FROM_LISTENER,

   @ProtoEnumValue(number = 2)
   EXCEPTION_FROM_INTERCEPTOR,

   @ProtoEnumValue(number = 3)
   ERROR_FROM_INTERCEPTOR
}
