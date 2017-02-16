package org.infinispan.distexec;

/**
 * DistributedTaskExecutionPolicy allows task to specify its custom task execution policy across
 * Infinispan cluster.
 * <p>
 * DistributedTaskExecutionPolicy effectively scopes execution of tasks to a subset of nodes. For
 * example, someone might want to exclusively execute tasks on a local network site instead of a
 * backup remote network centre as well. Others might, for example, use only a dedicated subset of a
 * certain Infinispan rack nodes for specific task execution. DistributedTaskExecutionPolicy is set
 * per instance of DistributedTask.
 *
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public enum DistributedTaskExecutionPolicy {

   ALL, SAME_MACHINE, SAME_RACK, SAME_SITE;
}
