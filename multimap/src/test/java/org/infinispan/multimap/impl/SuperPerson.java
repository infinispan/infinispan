package org.infinispan.multimap.impl;

import static java.util.Objects.hash;

import org.infinispan.test.data.Person;

public class SuperPerson extends Person {

   private static final long serialVersionUID = 4681502647114171590L;

   public SuperPerson(String name) {
      super(name);
   }

   @Override
   public boolean equals(Object o) {
      return super.equals(o) && o instanceof SuperPerson && ((SuperPerson) o).isSuper();

   }

   public boolean isSuper() {
      return true;
   }

   @Override
   public int hashCode() {
      return hash(super.hashCode(), isSuper());
   }

   @Override
   public String toString() {
      return "SuperPerson{" + "name='" + getName() + '\'' + ", isSuper=" + isSuper() + '}';
   }

}
