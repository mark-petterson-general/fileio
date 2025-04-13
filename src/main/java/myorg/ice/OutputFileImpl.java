package myorg.ice;

import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class OutputFileImpl extends FileImpl implements OutputFile {
    public OutputFileImpl(String filename) {
        super(filename);
    }

    private void makeFolder() {
        Path p = path().normalize().toAbsolutePath().getParent();
        try {
            Files.createDirectories(p);
        } catch (IOException e) {
            throw new RuntimeIOException("Getting or creating directory: " + p, e);
        }
    }

    private PositionOutputStream makeStream(SeekableByteChannel chan) {
        return new PositionOutputStream() {

            private long positionAfterClose = 0;

            @Override
            public long getPos() throws IOException {
                try {
                    return chan.position();
                } catch (ClosedChannelException e) {
                    // Iceberg can call this after the channel is closed
                    return positionAfterClose;
                }
            }

            @Override
            public void write(int b) throws IOException {
                byte[] bytes = {(byte) (b & 0xFF)};
                write(bytes, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                chan.write(
                        ByteBuffer.wrap(b, off, len)
                );
            }

            @Override
            public synchronized void close() throws IOException {
                try {
                    positionAfterClose = chan.position();
                } catch (ClosedChannelException e) {
                    // Iceberg can call position after handle is closed
                    // so save position for later.
                    // If closed is called again after already closing
                    // then do nothing.
                }
                chan.close();
            }
        };
    }

    @Override
    public PositionOutputStream create() {
        makeFolder();
        SeekableByteChannel writeChannel = openFile(
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        );
        return makeStream(writeChannel);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        makeFolder();
        SeekableByteChannel writeChannel = openFile(
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
        return makeStream(writeChannel);
    }

    @Override
    public InputFile toInputFile() {
        return new InputFileImpl(path().toString());
    }
}
