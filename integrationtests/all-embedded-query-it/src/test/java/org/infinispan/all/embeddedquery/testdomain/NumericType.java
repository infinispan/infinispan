package org.infinispan.all.embeddedquery.testdomain;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/**
 * A new additional entity type for testing Infinispan Querying.
 *
 * @author Anna Manukyan
 */
@Indexed(index = "person")
public class NumericType {

   @Field(store = Store.YES, analyze = Analyze.YES)
   private int num1;

   @Field(store = Store.YES, analyze = Analyze.YES)
   private int num2;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String name;

   public NumericType(int num1, int num2) {
      this.num1 = num1;
      this.num2 = num2;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NumericType that = (NumericType) o;

      if (num1 != that.num1) return false;
      if (num2 != that.num2) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = num1;
      result = 31 * result + num2;
      return result;
   }
}
