package org.infinispan.query.test;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Navin Surtani
 */
@Indexed(index = "person")
public class Person implements Serializable {

   private String name;

   private String blurb;

   private int age;

   private Date dateOfGraduation;

   private String nonIndexedField;

   public Person() {
   }

   public Person(String name, String blurb, int age) {
      this.name = name;
      this.blurb = blurb;
      this.age = age;
      this.nonIndexedField = name != null && name.length() >= 2 ? name.substring(0, 2) : null;
   }

   public Person(String name, String blurb, int age, Date dateOfGraduation) {
      this.name = name;
      this.blurb = blurb;
      this.age = age;
      this.dateOfGraduation = dateOfGraduation;
   }

   @Text
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
   @ProtoField(number = 3, defaultValue = "0")
   public int getAge() {
      return age;
   }

   public void setAge(int age) {
      this.age = age;
   }

   @ProtoField(number = 4)
   public String getNonIndexedField() {
      return nonIndexedField;
   }

   public void setNonIndexedField(String nonIndexedField) {
      this.nonIndexedField = nonIndexedField;
   }

   @Basic(projectable = true)
   @ProtoField(number = 5)
   public Date getDateOfGraduation() {
      return dateOfGraduation;
   }

   public void setDateOfGraduation(Date dateOfGraduation) {
      this.dateOfGraduation = dateOfGraduation;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Person person = (Person) o;
      return age == person.age && Objects.equals(name, person.name) && Objects.equals(blurb, person.blurb) && Objects.equals(dateOfGraduation, person.dateOfGraduation) && Objects.equals(nonIndexedField, person.nonIndexedField);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, blurb, age, dateOfGraduation, nonIndexedField);
   }

   @Override
   public String toString() {
      return "Person{name='" + name + "', blurb='" + blurb + "', age=" + age +
            ", dateOfGraduation=" + dateOfGraduation + ", nonIndexedField='" + nonIndexedField + "'}";
   }
}
