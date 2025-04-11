package myorg.ice;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parent class provides operations common to InputFile and OutputFile
 */
class FileImpl {

    private final Path path;

    protected FileImpl(String filename) {
        path = Paths.get(filename);
    }

    protected Path path() {
        return path;
    }

    protected SeekableByteChannel openFile(OpenOption... options) {
        try {
            return Files.newByteChannel(path, options);
        } catch (FileNotFoundException e) {
            throw new NotFoundException(errMessage(), e);
        } catch (FileAlreadyExistsException e) {
            throw new AlreadyExistsException(errMessage(), e);
        } catch (IOException e) {
            throw new RuntimeIOException(errMessage(), e);
        }
    }

    protected String errMessage() {
        return "Error accessing file: " + location();
    }

    public String location() {
        return path.normalize().toAbsolutePath().toString();
    }

    public boolean exists() {
        return Files.exists(path());
    }

}
