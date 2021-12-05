# Compressor

A [compressor](https://github.com/SETL-Developers/setl/blob/master/src/main/scala/com/jcdecaux/setl/storage/Compressor.scala)
can:
- compress a string to a byte array
- decompress a byte array to a string

## Example:

```scala
import io.github.setl.storage.GZIPCompressor

val compressor = new GZIPCompressor()

val compressed = compressor.compress("data to be compressed")
val data = compressor.decompress(compressed)
```  

# Archiver

An [Archiver](https://github.com/SETL-Developers/setl/blob/master/src/main/scala/com/jcdecaux/setl/storage/Archiver.scala) can
package files and directories into a single data archive file.

