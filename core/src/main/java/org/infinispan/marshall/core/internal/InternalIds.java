package org.infinispan.marshall.core.internal;

final class InternalIds {

   static final int PRIMITIVE         = 0x00; // primitives, including null
   static final int NON_PRIMITIVE     = 0x01; // internal and predefined user externalizers
   static final int ANNOTATED         = 0x02; // annotated with @SerializeWith or @SerializeFunctionWith
   static final int EXTERNAL          = 0x03; // external

   private InternalIds() {
   }

}
