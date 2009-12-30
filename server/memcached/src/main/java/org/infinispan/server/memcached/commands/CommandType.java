/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached.commands;

import java.io.IOException;

import org.infinispan.server.memcached.UnknownCommandException;

/**
 * Command.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public enum CommandType {
   SET, ADD, REPLACE, APPEND, PREPEND, CAS,
   GET, GETS,
   DELETE,
   INCR, DECR,
   STATS,
   FLUSH_ALL,
   VERSION,
   QUIT
   ;
   
   public boolean isStorage() {
      switch(this) {
         case SET:
         case ADD:
         case REPLACE:
         case APPEND:
         case PREPEND:
         case CAS:
            return true;
         default:
            return false;
      }
   }

   @Override
   public String toString() {
      return super.toString().toLowerCase();
   }

   static CommandType parseType(String type) throws IOException {
     if(type.equals(CommandType.SET.toString())) return SET;
     else if(type.equals(CommandType.ADD.toString())) return ADD;
     else if(type.equals(CommandType.REPLACE.toString())) return REPLACE;
     else if(type.equals(CommandType.APPEND.toString())) return APPEND;
     else if(type.equals(CommandType.PREPEND.toString())) return PREPEND;
     else if(type.equals(CommandType.CAS.toString())) return CAS;
     else if(type.equals(CommandType.GET.toString())) return GET;
     else if(type.equals(CommandType.GETS.toString())) return GETS;
     else if(type.equals(CommandType.DELETE.toString())) return DELETE;
     else if(type.equals(CommandType.INCR.toString())) return INCR;
     else if(type.equals(CommandType.DECR.toString())) return DECR;
     else if(type.equals(CommandType.STATS.toString())) return STATS;
     else if(type.equals(CommandType.FLUSH_ALL.toString())) return FLUSH_ALL;
     else if(type.equals(CommandType.VERSION.toString())) return VERSION;
     else if(type.equals(CommandType.QUIT.toString())) return QUIT;
     else throw new UnknownCommandException("request \"" + type + "\" not known");
   }


}
