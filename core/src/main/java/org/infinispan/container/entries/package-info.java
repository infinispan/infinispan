/**
 * Entries which are stored in data containers.  This package contains different implementations of
 * entries based on the information needed to store an entry.  Certain entries need more information - such as timestamps
 * and lifespans, if they are used - than others, and the appropriate implementation is selected dynamically.  This
 * helps minimize Infinispan's memory requirements without storing unnecessary metadata.
 */
package org.infinispan.container.entries;
