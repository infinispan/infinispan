package org.infinispan.server.memcached;

/**
 * Protocol decoding state
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public enum MemcachedDecoderState {
   DECODE_HEADER,
   DECODE_KEY,
   DECODE_PARAMETERS,
   DECODE_VALUE,
}
