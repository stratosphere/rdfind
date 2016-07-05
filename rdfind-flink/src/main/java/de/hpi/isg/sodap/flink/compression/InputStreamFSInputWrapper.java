package de.hpi.isg.sodap.flink.compression;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

/**
 * @author sebastian.kruse
 * @since 29.04.2015
 */
public class InputStreamFSInputWrapper extends FSDataInputStream {

    private InputStream inStream;

    public InputStreamFSInputWrapper(InputStream inStream) {
        this.inStream = inStream;
    }

    @Override
    public void seek(long desired) throws IOException {
        throw new UnsupportedOperationException("Wrapped stream: does not support the seek operation.");
    }

    @Override
    public int read() throws IOException {
        return inStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inStream.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return inStream.read(b);
    }

    @Override
    public long getPos() throws IOException {
        throw new RuntimeException("Not implemented.");
    }
}
