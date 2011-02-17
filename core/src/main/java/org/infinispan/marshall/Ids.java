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
 * Indexes for object types. These are currently limited to being unsigned ints, so valid values are considered those
 * in the range of 0 to 254. Please note that the use of 255 is forbidden since this is reserved for foreign, or user
 * defined, externalizers.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Ids {
   // No internal externalizer should use this upper limit Id or anything higher than that.
   static final int MAX_ID = 255;

   static final int ARRAY_LIST = 0;
   static final int LINKED_LIST = 1;
   static final int MAPS = 2;
   static final int JDK_SETS = 3;
   static final int SINGLETON_LIST = 4;
   // responses
   static final int SUCCESSFUL_RESPONSE = 5;
   static final int EXTENDED_RESPONSE = 6;
   static final int EXCEPTION_RESPONSE = 7;
   static final int UNSUCCESSFUL_RESPONSE = 8;
   static final int REQUEST_IGNORED_RESPONSE = 9;
   // entries and values
   static final int IMMORTAL_ENTRY = 10;
   static final int MORTAL_ENTRY = 11;
   static final int TRANSIENT_ENTRY = 12;
   static final int TRANSIENT_MORTAL_ENTRY = 13;
   static final int IMMORTAL_VALUE = 14;
   static final int MORTAL_VALUE = 15;
   static final int TRANSIENT_VALUE = 16;
   static final int TRANSIENT_MORTAL_VALUE = 17;
   // internal collections (id=18 no longer in use, might get reused at a later stage)
   static final int IMMUTABLE_MAP = 19;
   static final int ATOMIC_HASH_MAP = 20;
   // others
   static final int GLOBAL_TRANSACTION = 38;
   static final int JGROUPS_ADDRESS = 39;
   static final int MARSHALLED_VALUE = 40;
   static final int TRANSACTION_LOG_ENTRY = 41;
   static final int BUCKET = 42;
   static final int DEADLOCK_DETECTING_GLOBAL_TRANSACTION = 43;
   static final int REMOTE_TX_LOG_DETAILS = 63;

   // 44 and 45 no longer in use, used to belong to tree module
   static final int ATOMIC_HASH_MAP_DELTA = 46;
   static final int ATOMIC_PUT_OPERATION = 47;
   static final int ATOMIC_REMOVE_OPERATION = 48;
   static final int ATOMIC_CLEAR_OPERATION = 49;
   static final int DEFAULT_CONSISTENT_HASH = 51;
   static final int UNION_CONSISTENT_HASH = 52;
   static final int UNSURE_RESPONSE = 54;
   // 55 - 56 no longer in use since server modules can now register their own externalizers
   static final int BYTE_ARRAY_KEY = 57;
   // 58 - 59 no longer in use since server modules can now register their own externalizers
   static final int NODE_TOPOLOGY_INFO = 60;
   static final int TOPOLOGY_AWARE_CH = 61;
   // commands (ids between 21 and 37 both inclusive and 50 and 53, are no longer in use, might get reused at a later stage)
   static final int REPLICABLE_COMMAND = 62;
}
