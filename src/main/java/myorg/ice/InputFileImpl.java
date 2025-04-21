package myorg.ice;

import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;

public class InputFileImpl extends FileImpl implements InputFile {

    public InputFileImpl(String filename) {
        super(filename);
    }

    private SeekableInputStream makeStream(SeekableByteChannel chan) {
        return new SeekableInputStream() {
            @Override
            public long getPos() throws IOException {
                return chan.position();
            }

            @Override
            public void seek(long l) throws IOException {
                chan.position(l);
            }

            @Override
            public int read() throws IOException {
                byte[] b = new byte[1];
                if (read(b, 0, 1) == -1) {
                    return -1;
                }
                // sonarlint convert to unsigned byte
                return b[0] & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return chan.read(
                        ByteBuffer.wrap(b, off, len)
                );
            }

            @Override
            public void close() throws IOException {
                chan.close();
            }
        };
    }

    @Override
    public long getLength() {
        try {
            return (long) Files.getAttribute(path(), "size");
        } catch (IOException e) {
            throw new RuntimeIOException(errMessage(), e);
        }
    }

    @Override
    public SeekableInputStream newStream() {
        SeekableByteChannel readChannel = openFile(StandardOpenOption.READ);
        return makeStream(readChannel);
    }

}
