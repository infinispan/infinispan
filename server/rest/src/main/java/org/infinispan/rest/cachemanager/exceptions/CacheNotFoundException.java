package org.infinispan.rest.cachemanager.exceptions;

import org.infinispan.commons.CacheException;

public class CacheNotFoundException extends CacheException {
   public CacheNotFoundException(String msg) {
      super(msg);
   }
}
