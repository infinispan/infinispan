package org.infinispan.persistence.sifs;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryInfo extends EntryPosition {
   public final short numRecords;
   public final int cacheSegment;

   public EntryInfo(int file, int offset, short numRecords, int cacheSegment) {
      super(file, offset);
      this.numRecords = numRecords;
      this.cacheSegment = cacheSegment;
   }
}
