package org.infinispan.server.memcached;

/**
 * Protocol decoding state
 *
 * @author Galder Zamarreño
 * @since 4.2
 * @deprecated since 10.1. Will be removed unless a binary protocol encoder/decoder is implemented.
 */
@Deprecated
public enum MemcachedDecoderState {
   DECODE_HEADER,
   DECODE_KEY,
   DECODE_PARAMETERS,
   DECODE_VALUE,
}
