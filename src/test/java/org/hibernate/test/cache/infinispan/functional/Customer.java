<<<<<<< HEAD
<<<<<<< HEAD
/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2008, Red Hat, Inc. and/or it's affiliates or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat, Inc. and/or it's affiliates.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.test.cache.infinispan.functional;

import java.io.Serializable;
import java.util.Set;

/**
 * Company customer
 * 
 * @author Emmanuel Bernard
 * @author Kabir Khan
 */
public class Customer implements Serializable {
   Integer id;
   String name;

   private transient Set<Contact> contacts;

   public Customer() {
   }

   public Integer getId() {
      return id;
   }

   public void setId(Integer id) {
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public void setName(String string) {
      name = string;
   }

   public Set<Contact> getContacts() {
      return contacts;
   }

   public void setContacts(Set<Contact> contacts) {
      this.contacts = contacts;
   }
}
=======
/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2008, Red Hat, Inc. and/or it's affiliates or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat, Inc. and/or it's affiliates.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.test.cache.infinispan.functional;
import java.io.Serializable;
import java.util.Set;

/**
 * Company customer
 * 
 * @author Emmanuel Bernard
 * @author Kabir Khan
 */
public class Customer implements Serializable {
   Integer id;
   String name;

   private transient Set<Contact> contacts;

   public Customer() {
   }

   public Integer getId() {
      return id;
   }

   public void setId(Integer id) {
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public void setName(String string) {
      name = string;
   }

   public Set<Contact> getContacts() {
      return contacts;
   }

   public void setContacts(Set<Contact> contacts) {
      this.contacts = contacts;
   }
}
>>>>>>> Changes from requests at Hibernate meeting: message codes, use XXXf methods for debug and trace, use @Cause
=======
/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.test.cache.infinispan.functional;
import java.io.Serializable;
import java.util.Set;

/**
 * Company customer
 * 
 * @author Emmanuel Bernard
 * @author Kabir Khan
 */
public class Customer implements Serializable {
   Integer id;
   String name;

   private transient Set<Contact> contacts;

   public Customer() {
   }

   public Integer getId() {
      return id;
   }

   public void setId(Integer id) {
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public void setName(String string) {
      name = string;
   }

   public Set<Contact> getContacts() {
      return contacts;
   }

   public void setContacts(Set<Contact> contacts) {
      this.contacts = contacts;
   }
}
>>>>>>> HHH-9803 - Checkstyle fix ups - headers
