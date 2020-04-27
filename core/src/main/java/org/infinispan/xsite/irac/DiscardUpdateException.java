package org.infinispan.xsite.irac;

import org.infinispan.commons.CacheException;

/**
 * For optimistic transactions, it signals the update from the remote site is not valid (old version or conflict
 * resolution rejected it).
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class DiscardUpdateException extends CacheException {

   private static final DiscardUpdateException CACHED_INSTANCE = new DiscardUpdateException("Update Discarded", null,
         true, false);

   public DiscardUpdateException(String message, Throwable cause, boolean enableSuppression,
         boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }

   public static DiscardUpdateException getInstance() {
      return CACHED_INSTANCE;
   }
}
