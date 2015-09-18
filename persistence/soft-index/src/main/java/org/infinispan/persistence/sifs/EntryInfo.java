package org.infinispan.persistence.sifs;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EntryInfo extends EntryPosition {
   public final short numRecords;

   public EntryInfo(int file, int offset, short numRecords) {
      super(file, offset);
      this.numRecords = numRecords;
   }
}
