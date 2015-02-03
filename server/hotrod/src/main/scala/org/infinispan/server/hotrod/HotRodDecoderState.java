package org.infinispan.server.hotrod;

/**
 * Protocol decoding state
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public enum HotRodDecoderState {
   DECODE_HEADER,
   DECODE_KEY,
   DECODE_PARAMETERS,
   DECODE_VALUE,
}
