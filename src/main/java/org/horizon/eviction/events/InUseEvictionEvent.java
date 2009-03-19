package org.horizon.eviction.events;

/**
 * An eviction event used to mark an entry as in-use.
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class InUseEvictionEvent extends EvictionEvent {

   long inUseTimeout;

   public InUseEvictionEvent(Object key, long inUseTimeout) {
      super(key, Type.MARK_IN_USE_EVENT);
      this.inUseTimeout = inUseTimeout;
   }

   public long getInUseTimeout() {
      return inUseTimeout;
   }

   public void setInUseTimeout(long inUseTimeout) {
      this.inUseTimeout = inUseTimeout;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InUseEvictionEvent that = (InUseEvictionEvent) o;
      if (!super.equals(o)) return false;
      if (inUseTimeout != that.inUseTimeout) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int hc = super.hashCode();
      return hc * 31 + (int) (inUseTimeout ^ (inUseTimeout >>> 32));
   }

   @Override
   public String toString() {
      return "InUseEvictionEvent{" +
            "key=" + key +
            ", type=" + type +
            ", inUseTimeout=" + inUseTimeout +
            '}';
   }
}
