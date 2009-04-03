package org.horizon.container.entries;


/**
 * A null entry that is read in for removal
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NullMarkerEntryForRemoval extends RepeatableReadEntry {

   public NullMarkerEntryForRemoval(Object key) {
      super(key, null, -1);
   }

   /**
    * @return always returns true
    */
   @Override
   public final boolean isNull() {
      return true;
   }

   /**
    * @return always returns true so that any get commands, upon getting this entry, will ignore the entry as though it
    *         were removed.
    */
   @Override
   public final boolean isRemoved() {
      return true;
   }

   /**
    * @return always returns true so that any get commands, upon getting this entry, will ignore the entry as though it
    *         were invalid.
    */
   @Override
   public final boolean isValid() {
      return false;
   }
}