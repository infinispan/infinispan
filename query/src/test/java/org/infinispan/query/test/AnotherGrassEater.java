package org.infinispan.query.test;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "anotherclass")
public class AnotherGrassEater {

   @Field(store = Store.YES)
   private String name;
   @Field(store = Store.YES)
   private String blurb;

   @Field(store = Store.NO)
   @SortableField
   private int age;

   public AnotherGrassEater() {
   }

   public AnotherGrassEater(String name, String blurb) {
      this.name = name;
      this.blurb = blurb;
   }

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(number = 2)
   public String getBlurb() {
      return blurb;
   }

   public void setBlurb(String blurb) {
      this.blurb = blurb;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AnotherGrassEater that = (AnotherGrassEater) o;

      if (blurb != null ? !blurb.equals(that.blurb) : that.blurb != null) return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (blurb != null ? blurb.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "AnotherGrassEater{" +
              "name='" + name + '\'' +
              ", blurb='" + blurb + '\'' +
              '}';
   }
}
