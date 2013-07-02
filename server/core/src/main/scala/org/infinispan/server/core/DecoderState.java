package org.infinispan.server.core;

/**
 * Protocol decoding state
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public enum DecoderState {
   DECODE_HEADER,
   DECODE_KEY,
   DECODE_PARAMETERS,
   DECODE_VALUE,
}
