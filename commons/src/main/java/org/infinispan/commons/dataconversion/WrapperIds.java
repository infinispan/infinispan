package org.infinispan.commons.dataconversion;

/**
 * @since 9.2
 */
public interface WrapperIds {
   byte NO_WRAPPER = 0;

   byte BYTE_ARRAY_WRAPPER = 1;

   byte PROTOBUF_WRAPPER = 2;

   /**
    * @deprecated Replaced by PROTOBUF_WRAPPER. Will be removed in next minor version.
    */
   @Deprecated
   byte PROTOSTREAM_WRAPPER = PROTOBUF_WRAPPER;

   byte IDENTITY_WRAPPER = 3;
}
