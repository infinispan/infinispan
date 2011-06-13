/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat, Inc., and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,  
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


/**
 * An Enumeration -> List adaptor
 *  
 * @author Pete Muir
 */
public class EnumerationList<T> extends ForwardingList<T>
{
   // The enumeration as a list
   private final List<T> list = new ArrayList<T>();
   
   /**
    * Constructor
    * 
    * @param enumeration The enumeration
    */
   public EnumerationList(Enumeration<T> enumeration)
   {
      while (enumeration.hasMoreElements())
      {
         list.add(enumeration.nextElement());
      }
   }

   @Override
   protected List<T> delegate()
   {
      return list;
   }
}
