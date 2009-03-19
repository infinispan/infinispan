package org.horizon.container;

/**
 * A null entry that is read in for removal
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class NullMarkerEntryForRemoval extends RepeatableReadEntry {

   public NullMarkerEntryForRemoval(Object key) {
      super(key, null, -1);
   }

   /**
    * @return always returns true
    */
   @Override
   public boolean isNullEntry() {
      return true;
   }

   /**
    * @return always returns true so that any get commands, upon getting this entry, will ignore the entry as though it
    *         were removed.
    */
   @Override
   public boolean isDeleted() {
      return true;
   }

   /**
    * @return always returns true so that any get commands, upon getting this entry, will ignore the entry as though it
    *         were invalid.
    */
   @Override
   public boolean isValid() {
      return false;
   }

}
