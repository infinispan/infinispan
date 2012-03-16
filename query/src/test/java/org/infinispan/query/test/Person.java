/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.test;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.StringBridge;

import java.io.Serializable;

/**
 * @author Navin Surtani
 */
@ProvidedId(bridge = @FieldBridge(impl = StringBridge.class))
@Indexed(index = "person")
public class Person implements Serializable {
   @Field(store = Store.YES)
   private String name;
   @Field(store = Store.YES)
   private String blurb;
   @Field(store = Store.YES, analyze=Analyze.NO)
   private int age;
   private static final long serialVersionUID = 8251606115293644545L;

   public Person() {
   }

   public Person(String name, String blurb, int age) {
      this.name = name;
      this.blurb = blurb;
      this.age = age;
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

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Person person = (Person) o;

      if (blurb != null ? !blurb.equals(person.blurb) : person.blurb != null) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (name != null ? name.hashCode() : 0);
      result = 31 * result + (blurb != null ? blurb.hashCode() : 0);
      return result;
   }

   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", blurb='" + blurb + '\'' +
            '}';
   }
}
