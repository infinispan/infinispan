/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.search.annotations;

/**
 * Defines whether the field content should be analyzed.
 *
 * @author Hardy Ferentschik
 */
public enum Analyze {
   /**
    * Analyze the field content
    */
   YES,

   /**
    * Index field content as is (not analyzed)
    */
   NO
}
