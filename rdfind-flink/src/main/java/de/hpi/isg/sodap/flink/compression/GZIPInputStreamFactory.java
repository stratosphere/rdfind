package de.hpi.isg.sodap.flink.compression;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Factory for creating a {@link java.util.zip.GZIPInputStream} around an input stream.
 *
 * @author sebastian.kruse
 * @since 28.04.2015
 */
public class GZIPInputStreamFactory implements InflaterInputStreamFactory<GZIPInputStream> {

    private static GZIPInputStreamFactory INSTANCE = null;

    /**
     * Returns the singleton instance of this class.
     *
     * @return an instance of the class.
     */
    public static GZIPInputStreamFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new GZIPInputStreamFactory();
        }
        return INSTANCE;
    }

    @Override
    public GZIPInputStream create(InputStream in) throws IOException {
        return new GZIPInputStream(in);
    }

}
