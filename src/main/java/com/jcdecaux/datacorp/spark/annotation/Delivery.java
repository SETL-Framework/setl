package com.jcdecaux.datacorp.spark.annotation;

import com.jcdecaux.datacorp.spark.transformation.Deliverable;
import com.jcdecaux.datacorp.spark.workflow.DispatchManager;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation @Delivery indicates {@link DispatchManager} that the current field
 * or method is marked as an input and it will be injected during the runtime by the DispatchManager.
 * <p>
 * If multiple {@link Deliverable} of the same type were found in the delivery pool of DispatchManager, then
 * it will try to compare the producer of the Deliverable
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface Delivery {

    /**
     * Producer of the current delivery that will be use by DispatchManager in order to find the corresponding delivery
     */
    Class<?> producer() default java.lang.Object.class;

    /**
     * Indicates whether the current Delivery is optional or not
     */
    boolean optional() default false;
}
