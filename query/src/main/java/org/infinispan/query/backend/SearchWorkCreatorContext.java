package org.infinispan.query.backend;

/**
 * Use context pattern, so it can be easily extended / changed.
 *
 * @author Ales Justin
 */
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
