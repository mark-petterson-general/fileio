/*
Copyright (C) 2025 by Mark Petterson

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

 */
package myorg.ice;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class LocalFileIO implements FileIO {

    public LocalFileIO() {
        // must have a no-arg constructor to be dynamically loaded
        // initialize(Map<String, String> properties) will be called to complete initialization
    }

    @Override
    public InputFile newInputFile(String s) {
        return new InputFileImpl(s);
    }

    @Override
    public OutputFile newOutputFile(String s) {
        return new OutputFileImpl(s);
    }

    @Override
    public void deleteFile(String s) {
        Path p = Paths.get(s);
        String errMsg = "file: " + p.normalize().toAbsolutePath();
        try {
            Files.delete(p);
        } catch (NoSuchFileException e) {
            throw new NotFoundException(errMsg, e);
        } catch (IOException e) {
            throw new RuntimeIOException(errMsg, e);
        }
    }
}
