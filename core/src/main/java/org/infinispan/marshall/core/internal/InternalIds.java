package org.infinispan.marshall.core.internal;

final class InternalIds {

   static final int NULL                = 0x00; // null
   static final int INTERNAL            = 0x01; // internal, including primitives
   static final int PREDEFINED          = 0x02; // predefined foreign externalizers
   static final int ANNOTATED           = 0x03; // annotated with @SerializeWith or @SerializeFunctionWith
   static final int EXTERNAL            = 0x04; // external

   private InternalIds() {
   }

}
