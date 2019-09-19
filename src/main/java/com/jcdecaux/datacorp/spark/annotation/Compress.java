package com.jcdecaux.datacorp.spark.annotation;

import com.jcdecaux.datacorp.spark.storage.Compressor;
import com.jcdecaux.datacorp.spark.storage.XZCompressor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface Compress {

    Class<? extends Compressor> compressor() default XZCompressor.class;

}
