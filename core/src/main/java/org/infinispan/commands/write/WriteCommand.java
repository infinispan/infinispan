/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.commands.write;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;

import java.util.Set;

/**
 * A command that modifies the cache in some way
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface WriteCommand extends VisitableCommand, FlagAffectedCommand {
   /**
    * Some commands may want to provide information on whether the command was successful or not.  This is different
    * from a failure, which usually would result in an exception being thrown.  An example is a putIfAbsent() not doing
    * anything because the key in question was present.  This would result in a isSuccessful() call returning false.
    *
    * @return true if the command completed successfully, false otherwise.
    */
   boolean isSuccessful();

   /**
    * Certain commands only work based on a certain condition or state of the cache.  For example, {@link
    * org.infinispan.Cache#putIfAbsent(Object, Object)} only does anything if a condition is met, i.e., the entry in
    * question is not already present.  This method tests whether the command in question is conditional or not.
    *
    * @return true if the command is conditional, false otherwise
    */
   boolean isConditional();

   /**
    *
    * @return a collection of keys affected by this write command.  Some commands - such as ClearCommand - may return
    * an empty collection for this method.
    */
   Set<Object> getAffectedKeys();
}
