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

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.infinispan.commands.write.AbstractDataWriteCommand.checkIfWriteSkewNeeded;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClearCommand extends AbstractFlagAffectedCommand implements WriteCommand {

    public static final byte COMMAND_ID = 5;
    CacheNotifier notifier;

    //Pedro -- set of keys that needs write skew check
    private Set<Object> keysMarkedForWriteSkew = new HashSet<Object>();

    public ClearCommand() {
    }

    public ClearCommand(CacheNotifier notifier, Set<Flag> flags) {
        this.notifier = notifier;
        this.flags = flags;
    }

    public void init(CacheNotifier notifier) {
        this.notifier = notifier;
    }

    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitClearCommand(ctx, this);
    }

    public Object perform(InvocationContext ctx) throws Throwable {
        Collection<CacheEntry> cacheEntrySet = ctx.getLookedUpEntries().values();

        //Pedro -- initialize the key set
        if(ctx.isOriginLocal()) {
            keysMarkedForWriteSkew = new HashSet<Object>(cacheEntrySet.size());
        }

        for (CacheEntry e : cacheEntrySet) {
            if (e instanceof MVCCEntry) {
                MVCCEntry me = (MVCCEntry) e;

                Object k = me.getKey(), v = me.getValue();

                if(me instanceof ClusteredRepeatableReadEntry) {
                    checkIfWriteSkewNeeded((ClusteredRepeatableReadEntry) me, ctx.isOriginLocal(),
                            keysMarkedForWriteSkew);
                }

                notifier.notifyCacheEntryRemoved(k, v, true, ctx);
                me.setRemoved(true);
                me.setValid(false);
                notifier.notifyCacheEntryRemoved(k, null, false, ctx);
            }
        }
        return null;
    }

    public Object[] getParameters() {
        //Pedro -- send the key set, if it is not empty or null. otherwise send null
        return new Object[]{(keysMarkedForWriteSkew != null && keysMarkedForWriteSkew.isEmpty() ?
                null : keysMarkedForWriteSkew), flags};
    }

    public byte getCommandId() {
        return COMMAND_ID;
    }

    public void setParameters(int commandId, Object[] parameters) {
        if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid command id");

        //Pedro -- receive the key set
        keysMarkedForWriteSkew = (Set<Object>) parameters[0];

        //Pedro -- it sends a null value if the set is empty
        if(keysMarkedForWriteSkew == null) {
            keysMarkedForWriteSkew = Collections.emptySet();
        }

        if (parameters.length > 1) {
            this.flags = (Set<Flag>) parameters[1];
        }
    }

    public boolean shouldInvoke(InvocationContext ctx) {
        return true;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("ClearCommand{flags=")
                .append(flags)
                .append("}")
                .toString();
    }

    public boolean isSuccessful() {
        return true;
    }

    public boolean isConditional() {
        return false;
    }

    public Set<Object> getAffectedKeys() {
        return Collections.emptySet();
    }

    @Override
    public boolean isReturnValueExpected() {
        return false;
    }

    @Override
    public boolean ignoreCommandOnStatus(ComponentStatus status) {
        return false;
    }

}
