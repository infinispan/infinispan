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
package org.infinispan.server.memcached.interceptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.server.core.transport.ChannelHandlerContext;
import org.infinispan.server.core.Command;
import org.infinispan.server.core.CommandInterceptor;
import org.infinispan.server.core.InterceptorChain;
import org.infinispan.server.memcached.commands.TextCommand;

/**
 * TextProtocolInterceptorChain.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public class TextProtocolInterceptorChain implements InterceptorChain {

   private final TextCommandInterceptor firstInChain;

   public TextProtocolInterceptorChain(TextCommandInterceptor firstInChain) {
      this.firstInChain = firstInChain;
   }
   
   @Override
   public Object invoke(ChannelHandlerContext ctx, Command command) throws Throwable {
      return ((TextCommand) command).acceptVisitor(ctx, firstInChain);
   }

   public List<CommandInterceptor> getInterceptorsWhichExtend(Class<? extends CommandInterceptor> interceptorClass) {
      List<CommandInterceptor> result = new ArrayList<CommandInterceptor>();
      for (CommandInterceptor interceptor : asList()) {
         boolean isSubclass = interceptorClass.isAssignableFrom(interceptor.getClass());
         if (isSubclass) {
            result.add(interceptor);
         }
      }
      return result;
   }

   /**
    * Returns an unmofiable list with all the interceptors in sequence. If first in chain is null an empty list is
    * returned.
    */
   private List<CommandInterceptor> asList() {
      if (firstInChain == null) return Collections.emptyList();

      List<CommandInterceptor> retval = new LinkedList<CommandInterceptor>();
      CommandInterceptor tmp = firstInChain;
      do {
         retval.add(tmp);
         tmp = tmp.getNext();
      }
      while (tmp != null);
      return Collections.unmodifiableList(retval);
   }


}
