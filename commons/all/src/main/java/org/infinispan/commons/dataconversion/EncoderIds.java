package org.infinispan.commons.dataconversion;

/**
 * @since 9.2
 * @deprecated since 12.1, to be removed in a future version.
 */
@Deprecated(forRemoval = true)
public interface EncoderIds {
   short NO_ENCODER = 0;
   short IDENTITY = 1;
   short BINARY = 2;
   short UTF8 = 3;
   /**
    * @deprecated Since 11.0, will be removed with ISPN-9622
    */
   @Deprecated(forRemoval = true)
   short GLOBAL_MARSHALLER = 4;
   /**
    * @deprecated Since 11.0, will be removed in 14.0. Set the storage media type and use transcoding instead.
    */
   @Deprecated(forRemoval = true)
   short GENERIC_MARSHALLER = 5;
   /**
    * @deprecated Since 11.0, will be removed in 14.0. Set the storage media type and use transcoding instead.
    */
   @Deprecated(forRemoval = true)
   short JAVA_SERIALIZATION = 6;
}
