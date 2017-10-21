package org.infinispan.commons.dataconversion;

/**
 * A wrapper that does not change the content.
 */
public class IdentityWrapper implements Wrapper {

   public static final IdentityWrapper INSTANCE = new IdentityWrapper();

   private IdentityWrapper() {
   }

   @Override
   public Object wrap(Object obj) {
      return obj;
   }

   @Override
   public Object unwrap(Object obj) {
      return obj;
   }

   @Override
   public byte id() {
      return WrapperIds.IDENTITY_WRAPPER;
   }

   @Override
   public boolean isFilterable() {
      return true;
   }
}
