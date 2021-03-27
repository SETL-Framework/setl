package io.github.setl.annotation;

import io.github.setl.workflow.External;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation @Delivery indicates {@link io.github.setl.workflow.DeliverableDispatcher} that the current field
 * or method is marked as an input and it will be injected during the runtime by the DispatchManager.
 * <p>
 * If multiple {@link io.github.setl.transformation.Deliverable} of the same type were found in the delivery pool of DispatchManager, then
 * it will try to compare the producer of the Deliverable
 */
@InterfaceStability.Evolving
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface Delivery {

    /**
     * Producer of the current delivery that will be use by DispatchManager in order to find the corresponding delivery
     */
    Class<?> producer() default External.class;

    /**
     * Indicates whether the current Delivery is optional or not
     */
    boolean optional() default false;

    boolean autoLoad() default false;

    String condition() default "";

    String id() default "";
}
