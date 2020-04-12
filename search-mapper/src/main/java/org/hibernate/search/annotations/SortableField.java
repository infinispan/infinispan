/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.search.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Makes a property sortable.
 * <p>
 * A field for that property must be declared via the {@link Field} annotation from which the field bridge configuration
 * will be inherited. In the rare case that a property should be sortable but not searchable, declare a field which is
 * not indexed nor stored. Then only the sort field will be added to the document, but no standard index field.
 * <p>
 * Sorting on a field without a declared sort field will still work, but it will be slower and cause a higher memory
 * consumption. Therefore it's strongly recommended to declare each required sort field.
 *
 * @author Gunnar Morling
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
@Documented
@Repeatable(SortableFields.class)
public @interface SortableField {

   /**
    * @return the name of the field whose field bridge to apply to obtain the value of this sort field. Can be omitted
    * in case only a single field exists for the annotated property.
    */
   String forField() default "";
}
