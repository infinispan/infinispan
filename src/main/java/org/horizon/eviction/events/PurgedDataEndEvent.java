package org.horizon.eviction.events;

import java.util.Set;

/**
 * To be put on an eviction event queue after expired data has been purged
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class PurgedDataEndEvent extends EvictionEvent {
   Set<Object> keysPurged;

   public PurgedDataEndEvent(Set<Object> keysPurged) {
      super(null, EvictionEvent.Type.EXPIRED_DATA_PURGE_END);
      this.keysPurged = keysPurged;
   }

   public Set<Object> getKeysPurged() {
      return keysPurged;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      PurgedDataEndEvent that = (PurgedDataEndEvent) o;

      if (keysPurged != null ? !keysPurged.equals(that.keysPurged) : that.keysPurged != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (keysPurged != null ? keysPurged.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "PurgedDataEndEvent{" +
            "keysPurged=" + keysPurged +
            '}';
   }
}
