package org.infinispan.persistence.sifs;

/**
 * File-offset pair
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class EntryPosition {
   public final int file;
   public final int offset;

   public EntryPosition(int file, int offset) {
      this.file = file;
      this.offset = offset;
   }

   public boolean equals(long file, int offset) {
      return this.file == file && this.offset == offset;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof EntryPosition)) return false;

      EntryPosition entryPosition = (EntryPosition) o;

      if (file != entryPosition.file) return false;
      if (offset != entryPosition.offset) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = file;
      result = 31 * result + offset;
      return result;
   }

   @Override
   public String toString() {
      return String.format("[%d:%d]", file, offset);
   }
}
