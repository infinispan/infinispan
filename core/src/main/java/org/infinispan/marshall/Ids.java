/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.marshall;

/**
 * Indexes for object types. These are currently limited to being unsigned bytes, so valid values are considered those
 * in the range of 0 to 254. Please note that the use of 255 is forbidden since this is reserved for foreign, or user
 * defined, externalizers.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Ids {
   // No internal externalizer should use this upper limit Id or anything higher than that.
   static final int MAX_ID = 255;

   static final byte ARRAY_LIST = 0;
   static final byte LINKED_LIST = 1;
   static final byte MAPS = 2;
   static final byte JDK_SETS = 3;
   static final byte SINGLETON_LIST = 4;
   // responses
   static final byte SUCCESSFUL_RESPONSE = 5;
   static final byte EXTENDED_RESPONSE = 6;
   static final byte EXCEPTION_RESPONSE = 7;
   static final byte UNSUCCESSFUL_RESPONSE = 8;
   static final byte REQUEST_IGNORED_RESPONSE = 9;
   // entries and values
   static final byte IMMORTAL_ENTRY = 10;
   static final byte MORTAL_ENTRY = 11;
   static final byte TRANSIENT_ENTRY = 12;
   static final byte TRANSIENT_MORTAL_ENTRY = 13;
   static final byte IMMORTAL_VALUE = 14;
   static final byte MORTAL_VALUE = 15;
   static final byte TRANSIENT_VALUE = 16;
   static final byte TRANSIENT_MORTAL_VALUE = 17;
   // internal collections (id=18 no longer in use, might get reused at a later stage)
   static final byte IMMUTABLE_MAP = 19;
   static final byte ATOMIC_HASH_MAP = 20;
   // others
   static final byte GLOBAL_TRANSACTION = 38;
   static final byte JGROUPS_ADDRESS = 39;
   static final byte MARSHALLED_VALUE = 40;
   static final byte TRANSACTION_LOG_ENTRY = 41;
   static final byte BUCKET = 42;
   static final byte DEADLOCK_DETECTING_GLOBAL_TRANSACTION = 43;
   // 44 and 45 no longer in use, used to belong to tree module
   static final byte ATOMIC_HASH_MAP_DELTA = 46;
   static final byte ATOMIC_PUT_OPERATION = 47;
   static final byte ATOMIC_REMOVE_OPERATION = 48;
   static final byte ATOMIC_CLEAR_OPERATION = 49;
   static final byte DEFAULT_CONSISTENT_HASH = 51;
   static final byte UNION_CONSISTENT_HASH = 52;
   static final byte UNSURE_RESPONSE = 54;
   // 55 - 56 no longer in use since server modules can now register their own externalizers
   static final byte BYTE_ARRAY_KEY = 57;
   // 58 - 59 no longer in use since server modules can now register their own externalizers
   static final byte NODE_TOPOLOGY_INFO = 60;
   static final byte TOPOLOGY_AWARE_CH = 61;
   // commands (ids between 21 and 37 both inclusive and 50 and 53, are no longer in use, might get reused at a later stage)
   static final byte REPLICABLE_COMMAND = 62;

}
