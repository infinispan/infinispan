package org.infinispan.distribution.group;

import static org.infinispan.util.ReflectionUtil.invokeAccessibly;

import org.infinispan.util.CollectionFactory;
import org.infinispan.util.InfinispanCollections;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;

public class GroupManagerImpl implements GroupManager {
    
    private static interface GroupMetadata {
        
        GroupMetadata NONE = new GroupMetadata() {
            
            @Override
            public String getGroup(Object instance) {
                return null;
            }
            
        }; 
        
        String getGroup(Object instance);
        
    }
    
    private static class GroupMetadataImpl implements GroupMetadata {
        private final Method method;

        public GroupMetadataImpl(Method method) {
            if (!String.class.isAssignableFrom(method.getReturnType()))
                throw new IllegalArgumentException(Util.formatString("@Group method %s must return java.lang.String", method));
            if (method.getParameterTypes().length > 0)
                throw new IllegalArgumentException(Util.formatString("@Group method %s must have zero arguments", method));
            this.method = method;
        }

        @Override
        public String getGroup(Object instance) {
            return String.class.cast(invokeAccessibly(instance, method, Util.EMPTY_OBJECT_ARRAY));
        }
        
    }
    
    private static GroupMetadata createGroupMetadata(Class<?> clazz) {
        Collection<Method> possibleMethods = ReflectionUtil.getAllMethods(clazz, Group.class);
        if (possibleMethods.isEmpty())
            return GroupMetadata.NONE;
        else if (possibleMethods.size() == 1)
            return new GroupMetadataImpl(possibleMethods.iterator().next());
        else
            throw new IllegalStateException(Util.formatString("Cannot define more that one @Group method for class hierarchy rooted at %s", clazz.getName()));
    }

    private final ConcurrentMap<Class<?>, GroupMetadata> groupMetadataCache;
    private final List<Grouper<?>> groupers;
    
    public GroupManagerImpl(List<Grouper<?>> groupers) {
        this.groupMetadataCache = CollectionFactory.makeConcurrentMap();
        if (groupers != null)
            this.groupers = groupers;
        else
            this.groupers = InfinispanCollections.emptyList();
    }
    
    @Override
    public String getGroup(Object key) {
        GroupMetadata metadata = getMetadata(key);
        if (metadata != null) {
            return applyGroupers(metadata.getGroup(key), key);
        } else
            return applyGroupers(null, key);
    }
    
    private String applyGroupers(String group, Object key) {
        for (Grouper<?> grouper : groupers) {
            if (grouper.getKeyType().isAssignableFrom(key.getClass()))
                group = ((Grouper<Object>) grouper).computeGroup(key, group);
        }
        return group;
    }
    
    private GroupMetadata getMetadata(final Object key) {
        final Class<?> keyClass = key.getClass();
        GroupMetadata groupMetadata = groupMetadataCache.get(keyClass);
        if (groupMetadata == null) {
          //this is not ideal as it is possible for the group metadata to be redundantly calculated several times.
          //however profiling showed that using the Map<Class,Future> cache-approach is significantly slower on
          // the long run
           groupMetadata = createGroupMetadata(keyClass);
           GroupMetadata previous = groupMetadataCache.putIfAbsent(keyClass, groupMetadata);
           if (previous != null) {
               // in case another thread added a metadata already, discard what we created and reuse the existing.
               return previous;
           }
       }
       return groupMetadata;
    }

}
