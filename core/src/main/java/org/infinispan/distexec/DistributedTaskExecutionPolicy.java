/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
