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
package org.infinispan.profiling.testinternals;

import org.infinispan.tree.Fqn;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Helper class that will generate fqn's.
 *
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */


public class FqnGenerator {

   private static final Random r = new Random();

   public static Fqn createRandomFqn(int depth) {
      List<String> fqnElements = new ArrayList<String>(depth);
      for (int i = 0; i < depth; i++) fqnElements.add(Integer.toHexString(r.nextInt(Integer.MAX_VALUE)));
      return Fqn.fromList(fqnElements);
   }


}
