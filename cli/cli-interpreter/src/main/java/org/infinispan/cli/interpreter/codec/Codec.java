package org.infinispan.cli.interpreter.codec;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.metadata.Metadata;

public interface Codec {

   String getName();

   void setWhiteList(ClassWhiteList whiteList);

   Object encodeKey(Object key) throws CodecException;

   Object encodeValue(Object value) throws CodecException;

   Object decodeKey(Object key) throws CodecException;

   Object decodeValue(Object value) throws CodecException;

   Metadata encodeMetadata(Cache<?, ?> cache, Long expires, Long maxIdle);
}
