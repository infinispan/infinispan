/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.commands;

import java.util.Set;

import org.infinispan.context.Flag;

/**
 * Commands affected by Flags should carry them over to the remote nodes.
 * 
 * By implementing this interface the remote handler will read them out and restore in context;
 * flags should still be evaluated in the InvocationContext.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
public interface FlagAffectedCommand extends VisitableCommand, TopologyAffectedCommand, MetadataAwareCommand {
   
   /**
    * @return the Flags which where set in the context - only valid to invoke after {@link #setFlags(Set)}
    */
   Set<Flag> getFlags();
   
   /**
    * Use it to store the flags from the InvocationContext into the Command before remoting the Command.
    * @param flags
    */
   void setFlags(Set<Flag> flags);

   /**
    * Use it to store the flags from the InvocationContext into the Command before remoting the Command.
    * @param flags
    */
   void setFlags(Flag... flags);

   /**
    * Check whether a particular flag is present in the command
    *
    * @param flag to lookup in the command
    * @return true if the flag is present
    */
   boolean hasFlag(Flag flag);

}
