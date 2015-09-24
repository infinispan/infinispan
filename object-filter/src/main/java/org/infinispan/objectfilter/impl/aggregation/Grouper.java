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

   private final int[] groupFieldPositions;

   private final FieldAccumulator[] accumulators;

   private final int rowLength;

   /**
    * Store the a row for each group. This is used only if we have at least one grouping column.
    */
   private final LinkedHashMap<GroupRowKey, Object[]> groups;

   /**
    * The single row used for computing global aggregations (the are no grouping columns defined).
    */
   private Object[] globalGroup;

   private final class GroupRowKey {

      private final Object[] row;

      private final int[] groupFieldPositions;

      GroupRowKey(Object[] row, int[] groupFieldPositions) {
         this.row = row;
         this.groupFieldPositions = groupFieldPositions;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         GroupRowKey other = (GroupRowKey) o;
         for (int pos : groupFieldPositions) {
            Object o1 = row[pos];
            Object o2 = other.row[pos];
            if (!(o1 == null ? o2 == null : o1.equals(o2))) {
               return false;
            }
         }
         return true;
      }

      @Override
      public int hashCode() {
         int result = 1;
         for (int pos : groupFieldPositions) {
            Object e = row[pos];
            result = 31 * result + (e == null ? 0 : e.hashCode());
         }
         return result;
      }
   }

   /**
    * groupFieldPositions and accumulators must not have overlapping indices.
    */
   public Grouper(int[] groupFieldPositions, FieldAccumulator[] accumulators) {
      this.groupFieldPositions = groupFieldPositions != null && groupFieldPositions.length != 0 ? groupFieldPositions : null;
      this.accumulators = accumulators != null && accumulators.length != 0 ? accumulators : null;
      rowLength = (groupFieldPositions != null ? groupFieldPositions.length : 0) + (accumulators != null ? accumulators.length : 0);
      if (rowLength == 0) {
         throw new IllegalArgumentException("Must have at least one grouping or aggregated column");
      }
      groups = this.groupFieldPositions != null ? new LinkedHashMap<GroupRowKey, Object[]>() : null;
   }

   public void addRow(Object[] row) {
      if (row.length != rowLength) {
         throw new IllegalArgumentException("Row length mismatch");
      }
      if (groupFieldPositions != null) {
         GroupRowKey groupRowKey = new GroupRowKey(row, groupFieldPositions);
         Object[] existingGroup = groups.get(groupRowKey);
         if (existingGroup == null) {
            if (accumulators != null) {
               for (FieldAccumulator acc : accumulators) {
                  acc.init(row);
               }
            }
            groups.put(groupRowKey, row);
         } else {
            if (accumulators != null) {
               for (FieldAccumulator acc : accumulators) {
                  acc.update(row, existingGroup);
               }
            }
         }
      } else {
         // we have global aggregations only
         if (globalGroup == null) {
            globalGroup = row;
            for (FieldAccumulator acc : accumulators) {
               acc.init(row);
            }
         } else {
            for (FieldAccumulator acc : accumulators) {
               acc.update(row, globalGroup);
            }
         }
      }
   }

   public Iterator<Object[]> finish() {
      if (groups != null) {
         return new Iterator<Object[]>() {

            private final Iterator<Object[]> iterator = groups.values().iterator();

            @Override
            public boolean hasNext() {
               return iterator.hasNext();
            }

            @Override
            public Object[] next() {
               Object[] row = iterator.next();
               if (accumulators != null) {
                  for (FieldAccumulator acc : accumulators) {
                     acc.finish(row);
                  }
               }
               return row;
            }
         };
      } else {
         for (FieldAccumulator acc : accumulators) {
            acc.finish(globalGroup);
         }
         return Collections.singleton(globalGroup).iterator();
      }
   }

   @Override
   public String toString() {
      return "Grouper{" +
            "groupFieldPositions=" + Arrays.toString(groupFieldPositions) +
            ", accumulators=" + Arrays.toString(accumulators) +
            ", number of groups=" + groups.size() +
            '}';
   }
}
