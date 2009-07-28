package org.infinispan.config;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.ReflectionUtil;

/**
 * Adds named cache specific features to the {@link org.infinispan.config.AbstractConfigurationBean}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractNamedCacheConfigurationBean extends AbstractConfigurationBean {

   protected ComponentRegistry cr;
   
   @Inject
   public void inject(ComponentRegistry cr) {
      this.cr = cr;
      
      // then recurse into field of this component...
      List<Field> fields = ReflectionUtil.getFields(this.getClass(),AbstractNamedCacheConfigurationBean.class);
      for (Field field : fields) {         
         AbstractNamedCacheConfigurationBean fieldValueThis = null;
            try {
               field.setAccessible(true);               
               fieldValueThis = (AbstractNamedCacheConfigurationBean) field.get(this);
               if(fieldValueThis!=null){
                  fieldValueThis.inject(cr);
               }
            } catch (Exception e) {
               log.warn("Could not inject for field " + field + " in class " +fieldValueThis,e);
            }         
      }
      
      //and don't forget to recurse into collections of components...
      fields = ReflectionUtil.getFields(this.getClass(), Collection.class);
      for (Field field : fields) {
         Type genericType = field.getGenericType();
         if (genericType instanceof ParameterizedType) {
            ParameterizedType aType = (ParameterizedType) genericType;
            Type[] fieldArgTypes = aType.getActualTypeArguments();
            for (Type fieldArgType : fieldArgTypes) {
               Class<?> fieldArgClass = (Class<?>) fieldArgType;
               if (!(fieldArgClass.isPrimitive() || fieldArgClass.equals(String.class))) {
                  try {
                     field.setAccessible(true);
                     Collection<Object> c = (Collection<Object>) field.get(this);
                     for (Object nextThis : c) {
                        if (AbstractNamedCacheConfigurationBean.class.isAssignableFrom(nextThis.getClass())) {
                           ((AbstractNamedCacheConfigurationBean) nextThis).inject(cr);
                        } else {
                           //collection does not contain AbstractNamedCacheConfigurationBean, skip altogether
                           break;
                        }
                     }
                  } catch (Exception e) {
                     log.warn("Could not inject for field " + field + " in class " +field,e);
                  }
               }
            }
         }
      }
   }

   protected boolean hasComponentStarted() {
      return cr != null && cr.getStatus() != null && cr.getStatus() == ComponentStatus.RUNNING;
   }
}
