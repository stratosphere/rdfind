package de.hpi.isg.sodap.flink.compression;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

/**
 * Creates a new instance of a certain subclass of {@link InflaterInputStream}.
 *
 * @author sebastian.kruse
 * @since 28.04.2015
 */
public interface InflaterInputStreamFactory<T extends InflaterInputStream> {

    /**
     * Creates the input stream that wraps the given input stream.
     * @param in is the compressed input stream
     * @return the inflated input stream
     */
    T create(InputStream in) throws IOException;
}
