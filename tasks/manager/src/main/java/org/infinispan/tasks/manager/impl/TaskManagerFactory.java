package org.infinispan.tasks.manager.impl;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.tasks.manager.TaskManager;

@DefaultFactoryFor(classes = TaskManager.class)
public class TaskManagerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      return new TaskManagerImpl();
   }
}
