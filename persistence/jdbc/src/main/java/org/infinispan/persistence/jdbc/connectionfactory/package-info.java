/**
 * Connection factories for use with the JDBC Cache Store.  Simple connection factories delegate
 * to a data source if used within a Java EE environment, otherwise C3P0 pooling is used if
 * outside of a Java EE environment.
 */
package org.infinispan.persistence.jdbc.connectionfactory;