/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod.impl.protocol;

import java.nio.charset.Charset;

/**
 * Defines constants defined by Hot Rod specifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotRodConstants {

   static final short REQUEST_MAGIC = 0xA0;
   static final short RESPONSE_MAGIC = 0xA1;

   static final byte VERSION_10 = 10;
   static final byte VERSION_11 = 11;
   static final byte VERSION_12 = 12;

   //requests
   static final byte PUT_REQUEST = 0x01;
   static final byte GET_REQUEST = 0x03;
   static final byte PUT_IF_ABSENT_REQUEST = 0x05;
   static final byte REPLACE_REQUEST = 0x07;
   static final byte REPLACE_IF_UNMODIFIED_REQUEST = 0x09;
   static final byte REMOVE_REQUEST = 0x0B;
   static final byte REMOVE_IF_UNMODIFIED_REQUEST = 0x0D;
   static final byte CONTAINS_KEY_REQUEST = 0x0F;
   static final byte GET_WITH_VERSION = 0x11;
   static final byte CLEAR_REQUEST = 0x13;
   static final byte STATS_REQUEST = 0x15;
   static final byte PING_REQUEST = 0x17;
   static final byte BULK_GET_REQUEST = 0x19;
   static final byte GET_WITH_METADATA = 0x1B;


   //responses
   static final byte PUT_RESPONSE = 0x02;
   static final byte GET_RESPONSE = 0x04;
   static final byte PUT_IF_ABSENT_RESPONSE = 0x06;
   static final byte REPLACE_RESPONSE = 0x08;
   static final byte REPLACE_IF_UNMODIFIED_RESPONSE = 0x0A;
   static final byte REMOVE_RESPONSE = 0x0C;
   static final byte REMOVE_IF_UNMODIFIED_RESPONSE = 0x0E;
   static final byte CONTAINS_KEY_RESPONSE = 0x10;
   static final byte GET_WITH_VERSION_RESPONSE = 0x12;
   static final byte CLEAR_RESPONSE = 0x14;
   static final byte STATS_RESPONSE = 0x16;
   static final byte PING_RESPONSE = 0x18;
   static final byte BULK_GET_RESPONSE = 0x1A;
   static final byte GET_WITH_METADATA_RESPONSE = 0x1C;
   static final byte ERROR_RESPONSE = 0x50;

   //response status
   static final byte NO_ERROR_STATUS = 0x00;
   static final int INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81;
   static final int REQUEST_PARSING_ERROR_STATUS = 0x84;
   static final byte NOT_PUT_REMOVED_REPLACED_STATUS = 0x01;
   static final int UNKNOWN_COMMAND_STATUS = 0x82;
   static final int SERVER_ERROR_STATUS = 0x85;
   static final int KEY_DOES_NOT_EXIST_STATUS = 0x02;
   static final int UNKNOWN_VERSION_STATUS = 0x83;
   static final int COMMAND_TIMEOUT_STATUS = 0x86;


   static final byte CLIENT_INTELLIGENCE_BASIC = 0x01;
   static final byte CLIENT_INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   static final byte CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;
   Charset HOTROD_STRING_CHARSET = Charset.forName("UTF-8");

   static final byte[] DEFAULT_CACHE_NAME_BYTES = new byte[]{};

   static final byte INFINITE_LIFESPAN = 0x01;
   static final byte INFINITE_MAXIDLE = 0x02;
}
