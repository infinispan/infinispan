/**
 * Commands that operate on the cache, either locally or remotely.  This package contains the entire command object
 * model including interfaces and abstract classes.  Your starting point is probably {@link org.infinispan.commands.ReplicableCommand}, which
 * represents a command that can be used in RPC calls.
 * <p />
 * A sub-interface, {@link org.infinispan.commands.VisitableCommand}, represents commands that can be visited using the <a href="http://en.wikipedia.org/wiki/Visitor_pattern">visitor pattern</a>.
 * Most commands that relate to public {@link org.infinispan.Cache} API methods tend to be {@link org.infinispan.commands.VisitableCommand}s, and hence the
 * importance of this interface.
 * <p />
 * The {@link org.infinispan.commands.Visitor} interface is capable of visiting {@link org.infinispan.commands.VisitableCommand}s, and a useful abstract implementation
 * of {@link org.infinispan.commands.Visitor} is {@link org.infinispan.interceptors.DDAsyncInterceptor}, which allows you to create
 * interceptors that intercept command invocations adding aspects of behavior to a given invocation.
 *
 * @author Manik Surtani
 * @since 4.0
 */
package org.infinispan.commands;
