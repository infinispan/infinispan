/* 
 * Hibernate, Relational Persistence for Idiomatic Java
 * 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.api;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

@Indexed(index="indexA")
public class TestEntity {

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

   public TestEntity(String name, String surname, long id, String note) {
      this.name = name;
      this.surname = surname;
      this.id = id;
      this.note = note;
   }

   public String getName() {
      return name;
   }

   public String getSurname() {
      return surname;
   }

   public long getId() {
      return id;
   }

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
