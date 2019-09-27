package com.jcdecaux.datacorp.spark.annotation;

import com.jcdecaux.datacorp.spark.storage.Compressor;
import com.jcdecaux.datacorp.spark.storage.XZCompressor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * The annotation Compress will indicate <code>StructAnalyser</code> to register the metadata of the corresponding fields
 * into the output <code>StructType</code>. Then all annotated columns will be compressed by <code>SchemaConverter</code>
 * during the save process in <code>SparkRepository</code>
 * </p>
 *
 * <p>
 * By default, the compression algorithm is LZMA with the default compression level (=6). You can define other compressor
 * by implementing <code>com.jcdecaux.datacorp.storage.Compressor</code> interface.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface Compress {

    Class<? extends Compressor> compressor() default XZCompressor.class;

}
