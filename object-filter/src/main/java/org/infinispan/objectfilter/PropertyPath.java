package org.infinispan.objectfilter;

import org.hibernate.hql.ast.origin.hql.resolve.path.AggregationPropertyPath;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.Collections;
import java.util.List;

/**
 * Represents the path of a field, including the aggregation function if any.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class PropertyPath {

   public enum AggregationType {
      SUM, AVG, MIN, MAX, COUNT;

      public static AggregationType from(AggregationPropertyPath.Type aggregationType) {
         if (aggregationType == null) {
            return null;
         }
         switch (aggregationType) {
            case SUM:
               return PropertyPath.AggregationType.SUM;
            case AVG:
               return PropertyPath.AggregationType.AVG;
            case MIN:
               return PropertyPath.AggregationType.MIN;
            case MAX:
               return PropertyPath.AggregationType.MAX;
            case COUNT:
               return PropertyPath.AggregationType.COUNT;
            default:
               throw new IllegalStateException("Aggregation " + aggregationType.name() + " is not supported");
         }
      }
   }

   /**
    * Optional aggregation type.
    */
   private final AggregationType aggregationType;

   private final List<String> path;

   public PropertyPath(AggregationType aggregationType, List<String> path) {
      this.aggregationType = aggregationType;
      this.path = path;
   }

   public PropertyPath(AggregationType aggregationType, String propertyName) {
      this(aggregationType, Collections.singletonList(propertyName));
   }

   public AggregationType getAggregationType() {
      return aggregationType;
   }

   public List<String> getPath() {
      return path;
   }

   public String asStringPath() {
      if (path.isEmpty()) {    //todo [anistor] can it really be empty?
         return null;
      }
      return StringHelper.join(path, ".");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != PropertyPath.class) return false;
      PropertyPath that = (PropertyPath) o;
      return aggregationType == that.aggregationType && path.equals(that.path);
   }

   @Override
   public int hashCode() {
      return 31 * (aggregationType != null ? aggregationType.hashCode() : 0) + path.hashCode();
   }

   @Override
   public String toString() {
      return aggregationType != null ?
            aggregationType.name() + '(' + StringHelper.join(path, ".") + ')' : StringHelper.join(path, ".");
   }
}
