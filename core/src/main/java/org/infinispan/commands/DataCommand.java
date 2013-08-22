package org.infinispan.commands;


/**
 * Commands of this type manipulate data in the cache.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface DataCommand extends VisitableCommand, TopologyAffectedCommand, LocalFlagAffectedCommand {
   Object getKey();
}