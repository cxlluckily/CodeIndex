package me.roohom.source;

import me.roohom.bean.Person;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class BinaryFileSource extends RichSourceFunction<Person> {
    private volatile Boolean isRunning = true;
    private String filePath;

    public BinaryFileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext context) throws Exception {

        File file = new File(filePath);
        FileInputStream fi = new FileInputStream(file);
        ObjectInputStream objectInputStream = new ObjectInputStream(fi);
        int count = 0;
        try {
            while (isRunning) {
                Person person = (Person) objectInputStream.readObject();
                context.collect(person);
            }
        } catch (Exception e) {
            //do nothing
        }
        fi.close();
        objectInputStream.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
