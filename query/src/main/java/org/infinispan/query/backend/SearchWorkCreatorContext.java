package org.infinispan.query.backend;

//todo [anistor] this class will be removed in Infinispan 10

/**
 * Use context pattern, so it can be easily extended / changed.
 *
 * @author Ales Justin
 * @deprecated without replacement
 */
@Deprecated
public class SearchWorkCreatorContext {
   private Object previousValue;
   private Object currentValue;

   public SearchWorkCreatorContext(Object previousValue, Object currentValue) {
      this.previousValue = previousValue;
      this.currentValue = currentValue;
   }

   public Object getPreviousValue() {
      return previousValue;
   }

   public Object getCurrentValue() {
      return currentValue;
   }
}
