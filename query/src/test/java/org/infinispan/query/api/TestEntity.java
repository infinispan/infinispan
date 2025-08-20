package org.infinispan.query.api;

import java.util.Objects;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "indexA")
public class TestEntity {

   private final String name;

   private final String surname;

   private final long id;

   private final String note;

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

   @Basic(projectable = true)
   @ProtoField(1)
   public String getName() {
      return name;
   }

   @Basic(projectable = true)
   @ProtoField(2)
   public String getSurname() {
      return surname;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public long getId() {
      return id;
   }

   @ProtoField(4)
   public String getNote() {
      return note;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      TestEntity that = (TestEntity) o;
      return id == that.id && Objects.equals(name, that.name) && Objects.equals(surname, that.surname) && Objects.equals(note, that.note);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, surname, id, note);
   }
}
