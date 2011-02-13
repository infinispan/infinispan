/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
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
public interface FlagAffectedCommand {
   
   /**
    * @return the Flags which where set in the context - only valid to invoke after {@link #setFlags(Set)}
    */
   public Set<Flag> getFlags();
   
   /**
    * Use it to store the flags from the InvocationContext into the Command before remoting the Command.
    * @param flags
    */
   public void setFlags(Set<Flag> flags);

}
