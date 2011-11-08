package org.infinispan.distribution.group;

import static org.infinispan.util.ReflectionUtil.invokeAccessibly;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;

public class GroupManagerImpl implements GroupManager {
    
    private static interface GroupMetadata {
        
        static final GroupMetadata NONE = new GroupMetadata() {
            
            public String getGroup(Object instance) {
                return null;
            }
            
        }; 
        
        String getGroup(Object instance);
        
    }
    
    private static class GroupMetadataImpl implements GroupMetadata {
        
        
        private static Object[] EMPTY_ARGS = new Object[0];
        
        private final Method method;

        public GroupMetadataImpl(Method method) {
            if (!String.class.isAssignableFrom(method.getReturnType()))
                throw new IllegalArgumentException(Util.formatString("@Group method %s must return java.lang.String", method));
            if (method.getParameterTypes().length > 0)
                throw new IllegalArgumentException(Util.formatString("@Group method %s must jave zero arguments", method));
            this.method = method;
        }

        public String getGroup(Object instance) {
            return String.class.cast(invokeAccessibly(instance, method, EMPTY_ARGS));
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

    private final ConcurrentMap<Class<?>, Future<GroupMetadata>> groupMetadataCache;
    private final List<Grouper<?>> groupers;
    
    public GroupManagerImpl(List<Grouper<?>> groupers) {
        this.groupMetadataCache = new ConcurrentHashMap<Class<?>, Future<GroupMetadata>>();
        if (groupers != null)
            this.groupers = groupers;
        else
            this.groupers = Collections.emptyList();
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
    
    private GroupMetadata getMetadata(Object key) {
        final Class<?> keyClass = key.getClass();
        if (!groupMetadataCache.containsKey(keyClass)) {
            Callable<GroupMetadata> c = new Callable<GroupMetadata>() {
                
                @Override
                public GroupMetadata call() throws Exception {
                    return createGroupMetadata(keyClass);
                }
                
            };
            FutureTask<GroupMetadata> ft = new FutureTask<GroupMetadata>(c);
            if (groupMetadataCache.putIfAbsent(keyClass, ft) == null) {
                ft.run();
            }
        }
        try {
            return groupMetadataCache.get(keyClass).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error extracting @Group from class hierarchy", e);
        }
    }

}
