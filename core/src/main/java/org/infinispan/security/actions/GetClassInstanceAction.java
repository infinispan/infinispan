package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.commons.util.Util;

/**
 * GetClassInstanceAction.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class GetClassInstanceAction<T> implements PrivilegedAction<T> {
   private final Class<T> clazz;

   public GetClassInstanceAction(Class<T> clazz) {
      this.clazz = clazz;
   }

   @Override
   public T run() {
      return Util.getInstance(clazz);
   }
}
