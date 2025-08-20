package org.infinispan.query.test;

import java.io.Serializable;
import java.util.Objects;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "anotherclass")
public class AnotherGrassEater implements Serializable {

   private String name;

   private String blurb;

   private int age;

   public AnotherGrassEater() {
   }

   public AnotherGrassEater(String name, String blurb) {
      this.name = name;
      this.blurb = blurb;
   }

   @Basic(projectable = true)
   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Text
   @ProtoField(number = 2)
   public String getBlurb() {
      return blurb;
   }

   public void setBlurb(String blurb) {
      this.blurb = blurb;
   }

   @Basic(projectable = true, sortable = true)
   public int getAge() {
      return age;
   }

   public void setAge(int age) {
      this.age = age;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      AnotherGrassEater that = (AnotherGrassEater) o;
      return age == that.age && Objects.equals(name, that.name) && Objects.equals(blurb, that.blurb);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, blurb, age);
   }

   @Override
   public String toString() {
      return "AnotherGrassEater{" +
            "name='" + name + '\'' +
            ", blurb='" + blurb + '\'' +
            ", age=" + age +
            '}';
   }
}
