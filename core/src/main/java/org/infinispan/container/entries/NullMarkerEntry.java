package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

/**
 * A marker entry to represent a null for repeatable read, so that a read that returns a null can continue to return
 * null.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class NullMarkerEntry extends NullMarkerEntryForRemoval {

   private static final NullMarkerEntry INSTANCE = new NullMarkerEntry();

   private NullMarkerEntry() {
      super(null, null);
   }

   public static NullMarkerEntry getInstance() {
      return INSTANCE;
   }

   /**
    * A no-op.
    */
   @Override
   public final void copyForUpdate(DataContainer d, boolean localModeWriteSkewCheck) {
      // no op
   }
}
