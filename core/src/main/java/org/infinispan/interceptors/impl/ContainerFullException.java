package org.infinispan.interceptors.impl;

import org.infinispan.commons.CacheException;

/**
 * Exception that is thrown when exception based eviction is enabled and the cache is full
 * @author wburns
 * @since 9.0
 */
public class ContainerFullException extends CacheException {
   public ContainerFullException(String msg) {
      super(msg);
   }
}
