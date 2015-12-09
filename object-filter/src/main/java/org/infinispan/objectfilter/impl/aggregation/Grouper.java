package org.infinispan.objectfilter.impl.aggregation;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Groups rows by their grouping fields and computes aggregates.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class Grouper {

   /**
    * The number of columns at the beginning of the row that are used for grouping.
    */
   private final int noOfGroupingColumns;

   private final FieldAccumulator[] accumulators;

   private final boolean twoPhaseAcc;

   private final int inRowLength;

   private final int outRowLength;

   /**
    * Store the a row for each group. This is used only if we have at least one grouping column.
    */
   private final LinkedHashMap<GroupRowKey, Object[]> groups;

   /**
    * The single row used for computing global aggregations (the are no grouping columns defined).
    */
   private final Object[] globalGroup;

   private final class GroupRowKey {

      private final Object[] row;

      GroupRowKey(Object[] row) {
         this.row = row;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         GroupRowKey other = (GroupRowKey) o;
         int n = noOfGroupingColumns > 0 ? noOfGroupingColumns : row.length;
         for (int i = 0; i < n; i++) {
            Object o1 = row[i];
            Object o2 = other.row[i];
            if (!(o1 == null ? o2 == null : o1.equals(o2))) {
               return false;
            }
         }
         return true;
      }

      @Override
      public int hashCode() {
         int result = 1;
         int n = noOfGroupingColumns > 0 ? noOfGroupingColumns : row.length;
         for (int i = 0; i < n; i++) {
            Object e = row[i];
            result = 31 * result + (e == null ? 0 : e.hashCode());
         }
         return result;
      }
   }

   /**
    * noOfGroupingColumns and accumulators must not have overlapping indices.
    */
   public Grouper(int noOfGroupingColumns, FieldAccumulator[] accumulators, boolean twoPhaseAcc) {
      this.noOfGroupingColumns = noOfGroupingColumns;
      this.accumulators = accumulators != null && accumulators.length != 0 ? accumulators : null;
      this.twoPhaseAcc = twoPhaseAcc;
      inRowLength = findInRowLength(noOfGroupingColumns, accumulators);
      if (inRowLength == 0) {
         throw new IllegalArgumentException("Must have at least one grouping or aggregated column");
      }
      outRowLength = noOfGroupingColumns + (accumulators != null ? accumulators.length : 0);
      if (noOfGroupingColumns > 0) {
         groups = new LinkedHashMap<GroupRowKey, Object[]>();
         globalGroup = null;
      } else {
         groups = null;
         // we have global aggregations only
         globalGroup = new Object[outRowLength];
         for (FieldAccumulator acc : accumulators) {
            acc.init(globalGroup);
         }
      }
   }

   private int findInRowLength(int noOfGroupingColumns, FieldAccumulator[] accumulators) {
      int l = noOfGroupingColumns;
      if (accumulators != null) {
         for (FieldAccumulator acc : accumulators) {
            if (acc.inPos + 1 > l) {
               l = acc.inPos + 1;
            }
         }
      }
      return l;
   }

   public void addRow(Object[] row) {
      if (row.length != inRowLength) {
         throw new IllegalArgumentException("Row length mismatch");
      }
      if (noOfGroupingColumns > 0) {
         // compute grouping and aggregations
         GroupRowKey groupRowKey = new GroupRowKey(row);
         Object[] existingGroup = groups.get(groupRowKey);
         if (existingGroup == null) {
            existingGroup = new Object[outRowLength];
            System.arraycopy(row, 0, existingGroup, 0, noOfGroupingColumns);
            if (accumulators != null) {
               FieldAccumulator.init(existingGroup, accumulators);
               for (FieldAccumulator acc : accumulators) {
                  acc.init(existingGroup);
               }
            }
            groups.put(groupRowKey, existingGroup);
         }
         if (accumulators != null) {
            if (twoPhaseAcc) {
               FieldAccumulator.merge(row, existingGroup, accumulators);
            } else {
               FieldAccumulator.update(row, existingGroup, accumulators);
            }
         }
      } else {
         // we have global aggregations only
         if (twoPhaseAcc) {
            FieldAccumulator.merge(row, globalGroup, accumulators);
         } else {
            FieldAccumulator.update(row, globalGroup, accumulators);
         }
      }
   }

   public Iterator<Object[]> finish() {
      if (groups != null) {
         return new Iterator<Object[]>() {

            private final Iterator<Object[]> iterator = groups.values().iterator();

            @Override
            public void remove() {
               throw new UnsupportedOperationException("remove");
            }

            @Override
            public boolean hasNext() {
               return iterator.hasNext();
            }

            @Override
            public Object[] next() {
               Object[] row = iterator.next();
               if (accumulators != null) {
                  FieldAccumulator.finish(row, accumulators);
               }
               return row;
            }
         };
      } else {
         FieldAccumulator.finish(globalGroup, accumulators);
         return Collections.singleton(globalGroup).iterator();
      }
   }

   @Override
   public String toString() {
      return "Grouper{" +
            "noOfGroupingColumns=" + noOfGroupingColumns +
            ", inRowLength=" + inRowLength +
            ", outRowLength=" + outRowLength +
            ", accumulators=" + Arrays.toString(accumulators) +
            ", number of groups=" + groups.size() +
            '}';
   }
}
