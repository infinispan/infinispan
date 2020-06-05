package org.infinispan.query.queries;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/**
 * A new additional entity type for testing Infinispan Querying.
 *
 * @author Anna Manukyan
 */
@Indexed(index = "numeric")
public class NumericType {

   private int num1;

   private int num2;

   private String name;

   public NumericType(int num1, int num2) {
      this.num1 = num1;
      this.num2 = num2;
   }

   @Field(store = Store.YES, analyze = Analyze.YES)
   public int getNum1() {
      return num1;
   }

   @Field(store = Store.YES, analyze = Analyze.YES)
   public int getNum2() {
      return num2;
   }

   @Field(store = Store.YES, analyze = Analyze.NO)
   public String getName() {
      return name;
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
