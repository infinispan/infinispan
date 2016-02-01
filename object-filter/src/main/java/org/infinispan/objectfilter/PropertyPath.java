package org.infinispan.objectfilter;

import org.hibernate.hql.ast.origin.hql.resolve.path.AggregationPropertyPath;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.util.StringHelper;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * Represents the path of a field, including the aggregation function if any.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class PropertyPath {

   private static final Log log = Logger.getMessageLogger(Log.class, PropertyPath.class.getName());

   public enum AggregationType {
      SUM, AVG, MIN, MAX, COUNT;

      public static AggregationType from(AggregationPropertyPath.Type aggregationType) {
         if (aggregationType == null) {
            return null;
         }
         switch (aggregationType) {
            case SUM:
               return AggregationType.SUM;
            case AVG:
               return AggregationType.AVG;
            case MIN:
               return AggregationType.MIN;
            case MAX:
               return AggregationType.MAX;
            case COUNT:
               return AggregationType.COUNT;
            default:
               throw log.getAggregationTypeNotSupportedException(aggregationType.name());
         }
      }
   }

   /**
    * Optional aggregation type.
    */
   private final AggregationType aggregationType;

   private final String[] path;

   public PropertyPath(AggregationType aggregationType, List<String> path) {
      this(aggregationType, path.toArray(new String[path.size()]));
   }

   public PropertyPath(AggregationType aggregationType, String[] path) {
      this.aggregationType = aggregationType;
      this.path = path;
   }

   public PropertyPath(AggregationType aggregationType, String propertyName) {
      this(aggregationType, new String[]{propertyName});
   }

   public AggregationType getAggregationType() {
      return aggregationType;
   }

   public String[] getPath() {
      return path;
   }

   public String asStringPath() {
      if (path.length == 0) {    //todo [anistor] can it really be empty?
         return null;
      }
      return StringHelper.join(path);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != PropertyPath.class) return false;
      PropertyPath that = (PropertyPath) o;
      return aggregationType == that.aggregationType && Arrays.equals(path, that.path);
   }

   @Override
   public int hashCode() {
      return 31 * (aggregationType != null ? aggregationType.hashCode() : 0) + Arrays.hashCode(path);
   }

   @Override
   public String toString() {
      return aggregationType != null ?
            aggregationType.name() + '(' + StringHelper.join(path) + ')' : StringHelper.join(path);
   }
}
