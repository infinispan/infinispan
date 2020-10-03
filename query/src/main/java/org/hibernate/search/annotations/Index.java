/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.search.annotations;

/**
 * Defines how an {@link Field} should be indexed.
 *
 * @author Emmanuel Bernard
 * @author Hardy Ferentschik
 */
public enum Index {
   /**
    * Index the field value.
    */
   YES,

   /**
    * Do not index the field value. This field can thus not be searched,
    * but one can still access its contents provided it is
    * {@link Store stored}.
    */
   NO
}
