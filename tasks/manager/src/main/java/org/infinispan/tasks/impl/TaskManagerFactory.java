package org.infinispan.tasks.impl;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.tasks.TaskManager;

@DefaultFactoryFor(classes={TaskManager.class})
public class TaskManagerFactory extends AbstractComponentFactory implements
         AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public Object construct(String componentName) {
      return new TaskManagerImpl();
   }
}
