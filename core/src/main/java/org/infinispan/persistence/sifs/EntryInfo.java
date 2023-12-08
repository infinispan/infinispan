package org.infinispan.persistence.sifs;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryInfo extends EntryPosition {
   public final int numRecords;
   public final int cacheSegment;

   public EntryInfo(int file, int offset, int numRecords, int cacheSegment) {
      super(file, offset);
      this.numRecords = numRecords;
      this.cacheSegment = cacheSegment;
   }

   public String toString() {
      return String.format("[%d:%d] containing %d records in segment %d", file, offset, numRecords, cacheSegment);
   }
}
