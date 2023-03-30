package org.infinispan.server.memcached.binary;

/**
 * Memcache binary protocol constants.
 * {@link <a href="https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped">Memcached Binary Protocol</a>}
 *
 * @since 15.0
 */
public interface BinaryConstants {
   // Magic
   byte MAGIC_REQ = (byte) 0x80;
   byte MAGIC_RES = (byte) 0x81;

   int MAX_EXPIRATION = 60 * 60 * 24 * 30;

   String MEMCACHED_SASL_PROTOCOL = "memcached";
}
