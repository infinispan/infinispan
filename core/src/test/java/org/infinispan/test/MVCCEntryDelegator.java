package org.infinispan.test;

import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.test.fwk.CacheEntryDelegator;

public class MVCCEntryDelegator extends CacheEntryDelegator implements MVCCEntry {
   private final MVCCEntry delegate;

   public MVCCEntryDelegator(MVCCEntry delegate) {
      super(delegate);
      this.delegate = delegate;
   }

   @Override
   public void setExpired(boolean expired) {
      delegate.setExpired(expired);
   }

   @Override
   public boolean isExpired() {
      return delegate.isExpired();
   }

   @Override
   public void resetCurrentValue() {
      delegate.resetCurrentValue();
   }

   @Override
   public void updatePreviousValue() {
      delegate.updatePreviousValue();
   }

   @Override
   public MVCCEntry clone() {
      return (MVCCEntry) super.clone();
   }
}
