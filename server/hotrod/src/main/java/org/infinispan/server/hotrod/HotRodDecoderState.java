package org.infinispan.server.hotrod;

/**
 * Protocol decoding state
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public enum HotRodDecoderState {
   DECODE_HEADER,
   DECODE_HEADER_CUSTOM,
   DECODE_KEY,
   DECODE_KEY_CUSTOM,
   DECODE_PARAMETERS,
   DECODE_VALUE,
   DECODE_VALUE_CUSTOM,
}
