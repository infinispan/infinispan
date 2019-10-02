package org.infinispan.query.api;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index="indexA")
public class TestEntity implements Serializable {

   @Field(store = Store.YES, analyze=Analyze.NO)
   private String name;

   @Field(store = Store.YES)
   private String surname;

   private long id;

   private String note;

   public TestEntity(TestEntity e) {
      this.id = e.getId();
      this.name = e.getName();
      this.surname = e.getSurname();
      this.note = e.getNote();
   }

   @ProtoFactory
   public TestEntity(String name, String surname, long id, String note) {
      this.name = name;
      this.surname = surname;
      this.id = id;
      this.note = note;
   }

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   @ProtoField(number = 2)
   public String getSurname() {
      return surname;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public long getId() {
      return id;
   }

   @ProtoField(number = 4)
   public String getNote() {
      return note;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      TestEntity that = (TestEntity) o;

      if (id != that.id)
         return false;
      if (name != null ? !name.equals(that.name) : that.name != null)
         return false;
      if (note != null ? !note.equals(that.note) : that.note != null)
         return false;
      if (surname != null ? !surname.equals(that.surname)
            : that.surname != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + (int) (id ^ (id >>> 32));
      result = 31 * result + (note != null ? note.hashCode() : 0);
      return result;
   }
}
