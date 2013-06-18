/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.cdi.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.enterprise.util.AnnotationLiteral;
import javax.inject.Qualifier;

/**
 * <p>
 * A synthetic qualifier that can be used to replace other user-supplied
 * configuration at deployment.
 * </p>
 * <p/>
 * <p/>
 *
 * @author Stuart Douglas <stuart@baileyroberts.com.au>
 * @author Pete Muir
 */
@Retention(RetentionPolicy.RUNTIME)
@Qualifier
public @interface Synthetic {

    long index();

    String namespace();

    public static class SyntheticLiteral extends AnnotationLiteral<Synthetic> implements Synthetic {

        private final Long index;

        private final String namespace;

        public SyntheticLiteral(String namespace, Long index) {
            this.namespace = namespace;
            this.index = index;
        }

        public long index() {
            return index;
        }

        public String namespace() {
            return namespace;
        }

    }

    /**
     * <p>
     * Provides a unique Synthetic qualifier for the specified namespace
     * </p>
     * <p/>
     * <p>
     * {@link Provider} is thread safe.
     * </p>
     *
     * @author Pete Muir
     */
    public static class Provider {

        // Map of generic Object to a SyntheticQualifier
        private final Map<Object, Synthetic> synthetics;
        private final String namespace;

        private AtomicLong count;

        public Provider(String namespace) {
            this.synthetics = new HashMap<Object, Synthetic>();
            this.namespace = namespace;
            this.count = new AtomicLong();
        }

        /**
         * Get a synthetic qualifier. The provided annotation is used to map the
         * generated qualifier, allowing later retrieval.
         *
         * @param annotation
         * @return
         */
        public Synthetic get(Object object) {
            // This may give us data races, but these don't matter as Annotation use it's type and it's members values
            // for equality, it does not use instance equality.
            // Note that count is atomic
            if (!synthetics.containsKey(object)) {
                synthetics.put(object, new Synthetic.SyntheticLiteral(namespace, count.getAndIncrement()));
            }
            return synthetics.get(object);
        }

        /**
         * Get a synthetic qualifier. The qualifier will not be stored for later
         * retrieval.
         *
         * @return
         */
        public Synthetic get() {
            return new Synthetic.SyntheticLiteral(namespace, count.getAndIncrement());
        }

        public void clear() {
            this.synthetics.clear();
        }
    }

}
