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
 * Indexes.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Ids {
   /**
    * ids for jdk classes *
    */

   static final byte ARRAY_LIST = 0;
   static final byte LINKED_LIST = 1;
   static final byte JDK_MAPS = 2;
   static final byte JDK_SETS = 3;
   static final byte SINGLETON_LIST = 4;

   /**
    * ids for infinispan core classes *
    */

   // responses
   static final byte SUCCESSFUL_RESPONSE = 5;
   static final byte EXTENDED_RESPONSE = 6;
   static final byte EXCEPTION_RESPONSE = 7;
   static final byte UNSUCCESSFUL_RESPONSE = 8;
   static final byte REQUEST_IGNORED_RESPONSE = 9;
   static final byte UNSURE_RESPONSE = 54;

   // entries and values
   static final byte IMMORTAL_ENTRY = 10;
   static final byte MORTAL_ENTRY = 11;
   static final byte TRANSIENT_ENTRY = 12;
   static final byte TRANSIENT_MORTAL_ENTRY = 13;
   static final byte IMMORTAL_VALUE = 14;
   static final byte MORTAL_VALUE = 15;
   static final byte TRANSIENT_VALUE = 16;
   static final byte TRANSIENT_MORTAL_VALUE = 17;

   // internal collections
   static final byte FASTCOPY_HASH_MAP = 18;
   static final byte IMMUTABLE_MAP = 19;
   static final byte ATOMIC_HASH_MAP = 20;

   // commands
   static final byte STATE_TRANSFER_CONTROL_COMMAND = 21;
   static final byte CLUSTERED_GET_COMMAND = 22;
   static final byte MULTIPLE_RPC_COMMAND = 23;
   static final byte SINGLE_RPC_COMMAND = 24;
   static final byte GET_KEY_VALUE_COMMAND = 25;
   static final byte PUT_KEY_VALUE_COMMAND = 26;
   static final byte REMOVE_COMMAND = 27;
   static final byte INVALIDATE_COMMAND = 28;
   static final byte REPLACE_COMMAND = 29;
   static final byte CLEAR_COMMAND = 30;
   static final byte PUT_MAP_COMMAND = 31;
   static final byte PREPARE_COMMAND = 32;
   static final byte COMMIT_COMMAND = 33;
   static final byte ROLLBACK_COMMAND = 34;
   static final byte INVALIDATE_L1_COMMAND = 35;
   static final byte LOCK_CONTROL_COMMAND = 36;
   static final byte EVICT_COMMAND = 37;
   // others

   static final byte GLOBAL_TRANSACTION = 38;
   static final byte JGROUPS_ADDRESS = 39;
   static final byte MARSHALLED_VALUE = 40;
   static final byte TRANSACTION_LOG_ENTRY = 41;
   static final byte BUCKET = 42;
   static final byte DEADLOCK_DETECTING_GLOBAL_TRANSACTION = 43;
   /**
    * ids for infinispan tree classes *
    */

   static final byte NODE_KEY = 44;

   static final byte FQN = 45;
   static final byte ATOMIC_HASH_MAP_DELTA = 46;

   static final byte ATOMIC_PUT_OPERATION = 47;
   static final byte ATOMIC_REMOVE_OPERATION = 48;
   static final byte ATOMIC_CLEAR_OPERATION = 49;
   static final byte REHASH_CONTROL_COMMAND = 50;

   static final byte DEFAULT_CONSISTENT_HASH = 51;
   static final byte UNION_CONSISTENT_HASH = 52;
   static final byte JOIN_COMPLETE_COMMAND = 53;
   /*
    * ids for server modules
    */

   static final byte SERVER_CACHE_VALUE = 55;
   static final byte MEMCACHED_CACHE_VALUE = 56;
   static final byte BYTE_ARRAY_KEY = 57;
   static final byte TOPOLOGY_ADDRESS = 58;
   static final byte TOPOLOGY_VIEW = 59;
   static final byte NODE_TOPOLOGY_INFO = 60;
   static final byte TOPOLOGY_AWARE_CH = 61;
}
