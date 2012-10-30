/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved.
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

import java.util.UUID;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * CancellationService manages association of Thread executing CancellableCommand in a remote VM and
 * if needed cancels command execution
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface CancellationService {

   /**
    * Registers thread with {@link CancellationService} under the given UUID id
    * 
    * @param t
    *           thread to associate with id
    * @param id
    *           chosen UUID id
    */
   public void register(Thread t, UUID id);

   /**
    * Unregisters thread with {@link CancellationService} given an id
    * 
    * @param id
    *           thread id
    */
   public void unregister(UUID id);

   /**
    * Cancels (invokes Thread#interrupt) a thread given a thread id
    * 
    * @param id
    *           thread id
    */
   public void cancel(UUID id);

}
