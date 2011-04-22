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
package org.infinispan.loaders.jdbc.stringbased;

import java.io.Serializable;

/**
 * Pojo used for testing jdbc caches stores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class Person implements Serializable {
   
   /** The serialVersionUID */
   private static final long serialVersionUID = -835015913569270262L;
   
   private String name;
   private String surname;
   private int age;
   private int hashCode = -1;

   public Person(String name, String surname, int age) {
      this.name = name;
      this.surname = surname;
      this.age = age;
   }

   public String getName() {
      return name;
   }

   public String getSurname() {
      return surname;
   }

   public int getAge() {
      return age;
   }

   public void setHashCode(int hashCode) {
      this.hashCode = hashCode;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Person)) return false;

      Person person = (Person) o;

      if (age != person.age) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;
      if (surname != null ? !surname.equals(person.surname) : person.surname != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      if (hashCode != -1) {
         return hashCode;
      }
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + age;
      return result;
   }
}
