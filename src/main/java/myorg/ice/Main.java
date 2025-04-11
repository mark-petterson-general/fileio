package myorg.ice;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        LocalFileIO f = new LocalFileIO();
        InputFile f1 = f.newInputFile("sample1.txt");
        System.out.println(f1.exists());
        System.out.println(f1.location());
        try (SeekableInputStream s1 = f1.newStream()) {
            s1.seek(3);
            byte[] bytes = s1.readAllBytes();
            System.out.println(UTF_8.decode(ByteBuffer.wrap(bytes)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        OutputFile f2 = f.newOutputFile("out2.txt");
        try (PositionOutputStream s2 = f2.create()) {
            s2.write(67);
        } catch (AlreadyExistsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        OutputFile f3 = f.newOutputFile("out3.txt");
        try (PositionOutputStream s3 = f3.createOrOverwrite()) {
            s3.write(75);
        } catch (IOException e) {
            e.printStackTrace();
        }

        OutputFile f4 = f.newOutputFile("tmp_???fold/sub1/out3.txt");
        try (PositionOutputStream s4 = f4.createOrOverwrite()) {
            s4.write(75);
        } catch (IOException e) {
            e.printStackTrace();
        }

        f.deleteFile("this_NOT_exists.file");

    }
}