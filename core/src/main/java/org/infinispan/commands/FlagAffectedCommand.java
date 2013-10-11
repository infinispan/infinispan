package org.infinispan.commands;

/**
 * Commands affected by Flags should carry them over to the remote nodes.
 * 
 * By implementing this interface the remote handler will read them out and restore in context;
 * flags should still be evaluated in the InvocationContext.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
public interface FlagAffectedCommand extends VisitableCommand, TopologyAffectedCommand, MetadataAwareCommand,
                                             LocalFlagAffectedCommand {
}
