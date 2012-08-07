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
package org.infinispan.query;

/**
 * The Query module is using custom RPC commands; to make sure the used command ids
 * are unique all numbers are defined here, and should stay in the range 100-119
 * which is the reserved range for this module.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
public interface ModuleCommandIds {

   public static final byte CLUSTERED_QUERY = 101;

   public static final byte UPDATE_INDEX = 102;

}
