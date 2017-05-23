package org.infinispan.rest.server.operations.mediatypes;

import org.infinispan.CacheSet;
import org.infinispan.rest.server.operations.exceptions.ServerInternalException;

public interface OutputPrinter {

   byte[] print(String cacheName, CacheSet<?> cacheSet, Charset charset) throws ServerInternalException;

   byte[] print(byte[] value, Charset charset) throws ServerInternalException;
}
