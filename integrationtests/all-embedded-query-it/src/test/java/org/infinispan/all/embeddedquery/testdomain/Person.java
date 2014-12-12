package org.infinispan.all.embeddedquery.testdomain;

import org.hibernate.search.annotations.*;
import org.hibernate.search.bridge.builtin.IntegerBridge;
import org.hibernate.search.bridge.builtin.StringBridge;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Navin Surtani
 */
@ProvidedId(bridge = @FieldBridge(impl = StringBridge.class))
@Indexed(index = "person")
@FullTextFilterDefs({
      @FullTextFilterDef(name = "personFilter", impl = PersonBlurbFilterFactory.class, cache = FilterCacheModeType.INSTANCE_AND_DOCIDSETRESULTS),
      @FullTextFilterDef(name = "personAgeFilter", impl = PersonAgeFilterFactory.class, cache = FilterCacheModeType.INSTANCE_AND_DOCIDSETRESULTS)
})
public class Person implements Serializable {
   @Field(store = Store.YES)
   private String name;
   @Field(store = Store.YES)
   private String blurb;
   @Field(store = Store.YES, analyze = Analyze.NO, bridge = @FieldBridge(impl = IntegerBridge.class))
   private int age;
   @Field(store = Store.YES, analyze = Analyze.NO)
   @DateBridge(resolution = Resolution.DAY)
   private Date dateOfGraduation;

   private String nonSearchableField;
   private static final long serialVersionUID = 8251606115293644545L;

   public Person() {
   }

   public Person(String name, String blurb, int age) {
      this.name = name;
      this.blurb = blurb;
      this.age = age;
   }

   public Person(String name, String blurb, int age, Date dateOfGraduation) {
      this.name = name;
      this.blurb = blurb;
      this.age = age;
      this.dateOfGraduation = dateOfGraduation;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getBlurb() {
      return blurb;
   }

   public void setBlurb(String blurb) {
      this.blurb = blurb;
   }

   public int getAge() {
      return age;
   }

   public void setAge(int age) {
      this.age = age;
   }

   public String getNonSearchableField() {
      return nonSearchableField;
   }

   public void setNonSearchableField(String nonSearchableField) {
      this.nonSearchableField = nonSearchableField;
   }

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

      if (age != person.age) return false;
      if (blurb != null ? !blurb.equals(person.blurb) : person.blurb != null) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;
      if (dateOfGraduation != null ? !dateOfGraduation.equals(person.dateOfGraduation) : person.dateOfGraduation != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      result = (name != null ? name.hashCode() : 0);
      result = 31 * result + (blurb != null ? blurb.hashCode() : 0);
      result = 31 * result + (dateOfGraduation != null ? dateOfGraduation.hashCode() : 0);
      result = 31 * result + age;
      return result;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", blurb='" + blurb + '\'' +
            ", age=" + age +
            ", dateOfGraduation=" + dateOfGraduation +
            '}';
   }
}
