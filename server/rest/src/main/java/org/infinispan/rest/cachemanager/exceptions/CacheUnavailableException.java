package org.infinispan.rest.cachemanager.exceptions;

import org.infinispan.commons.CacheException;

public class CacheUnavailableException extends CacheException {
   public CacheUnavailableException(String msg) {
      super(msg);
   }
}
