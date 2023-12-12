package org.infinispan.persistence.sifs;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryInfo extends EntryPosition {
   public final int numRecords;

   public EntryInfo(int file, int offset, int numRecords) {
      super(file, offset);
      this.numRecords = numRecords;
   }

   public String toString() {
      return String.format("[%d:%d] containing %d records", file, offset, numRecords);
   }
}
