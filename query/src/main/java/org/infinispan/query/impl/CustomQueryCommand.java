package org.infinispan.query.impl;


/**
 * Custom commands from the Query module should implement this interface
 * to fetch needed components.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
public interface CustomQueryCommand {

   void fetchExecutionContext(CommandInitializer ci);

}
