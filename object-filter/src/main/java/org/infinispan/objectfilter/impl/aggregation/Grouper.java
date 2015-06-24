package org.infinispan.objectfilter.impl.aggregation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Groups rows by their key fields and computes aggregates.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class Grouper {

   private final int[] groupFieldPositions;

   private final FieldAccumulator[] accumulators;

   private final int rowLength;

   private final LinkedHashMap<GroupRowKey, Object[]> groups = new LinkedHashMap<GroupRowKey, Object[]>();

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
         if (groupFieldPositions != null && groupFieldPositions.length != 0) {
            for (int pos : groupFieldPositions) {
               Object o1 = row[pos];
               Object o2 = other.row[pos];
               if (!(o1 == null ? o2 == null : o1.equals(o2))) {
                  return false;
               }
            }
         }
         return true;
      }

      @Override
      public int hashCode() {
         int result = 1;
         if (groupFieldPositions != null && groupFieldPositions.length != 0) {
            for (int pos : groupFieldPositions) {
               Object e = row[pos];
               result = 31 * result + (e == null ? 0 : e.hashCode());
            }
         }
         return result;
      }
   }

   public Grouper(int[] groupFieldPositions, FieldAccumulator[] accumulators) {
      rowLength = (groupFieldPositions != null ? groupFieldPositions.length : 0) + (accumulators != null ? accumulators.length : 0);
      if (rowLength == 0) {
         throw new IllegalArgumentException("Must have at least one grouping or aggregated column");
      }
      this.groupFieldPositions = groupFieldPositions;
      this.accumulators = accumulators;
      //todo [anistor] check groupFieldPositions and accumulators indices do not overlap. can they be repeated?
   }

   public void addRow(Object[] row) {
      if (row.length != rowLength) {
         throw new IllegalArgumentException("Row length mismatch");
      }
      GroupRowKey groupRowKey = new GroupRowKey(row);
      Object[] existingGroup = groups.get(groupRowKey);
      if (existingGroup == null) {
         if (accumulators != null && accumulators.length != 0) {
            for (FieldAccumulator acc : accumulators) {
               acc.init(row);
            }
         }
         groups.put(groupRowKey, row);
      } else {
         if (accumulators != null && accumulators.length != 0) {
            for (FieldAccumulator acc : accumulators) {
               acc.update(row, existingGroup);
            }
         }
      }
   }

   public Iterator<Object[]> getGroupIterator() {
      return new Iterator<Object[]>() {

         private final Iterator<Object[]> iterator = groups.values().iterator();

         @Override
         public boolean hasNext() {
            return iterator.hasNext();
         }

         @Override
         public Object[] next() {
            Object[] row = iterator.next();
            if (accumulators != null && accumulators.length != 0) {
               for (FieldAccumulator acc : accumulators) {
                  acc.finish(row);
               }
            }
            return row;
         }
      };
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
