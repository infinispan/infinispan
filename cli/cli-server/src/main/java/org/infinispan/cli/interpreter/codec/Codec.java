package org.infinispan.cli.interpreter.codec;

public interface Codec {

   String getName();

   Object encodeKey(Object key) throws CodecException;

   Object encodeValue(Object value) throws CodecException;

   Object decodeKey(Object key) throws CodecException;

   Object decodeValue(Object value) throws CodecException;
}
