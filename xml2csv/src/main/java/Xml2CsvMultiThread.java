import com.opencsv.CSVWriter;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName Xml2CsvMultiThread
 * @Author LYleonard
 * @Date 2020/4/19 1:57
 * @Description TODO
 * Version 1.0
 **/
public class Xml2CsvMultiThread extends Thread {

    private static final CountDownLatch countDownLatch = new CountDownLatch(10);
    private int fileIndex;
    private File[] files;
    private String[] fields;
    private String outpath;

    public String getOutpath() {
        return outpath;
    }

    public void setOutpath(String outpath) {
        this.outpath = outpath;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public void setFileIndex(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    public File[] getFiles() {
        return files;
    }

    public void setFiles(File[] files) {
        this.files = files;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    @Override
    public void run() {
        for (int i = 0; i < files.length; ++i) {
            if (i % 10 == fileIndex) {
                File eachFile = files[i];
                FileInputStream inputStream = null;
                try {
                    inputStream = new FileInputStream(eachFile);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                try {
                    SAXReader reader = new SAXReader();
                    Document document = reader.read(inputStream);
                    Element table = document.getRootElement();
                    Iterator<Element> it = table.elementIterator();
                    File csvfile = new File(eachFile.getPath()
                            .substring(0, eachFile.getPath().lastIndexOf(".")) + ".csv");
                    Writer writer = new FileWriter(csvfile);
                    CSVWriter csvWriter = new CSVWriter(writer);

                    traverseElement(it, csvWriter);
                    eachFile.renameTo(new File(outpath + eachFile.getName()));
                    csvWriter.close();
                } catch (DocumentException e) {
                    try {
                        inputStream.close();
                    } catch (IOException e1) {
                        System.out.println("文件输入流inputStream，关闭失败！");
                        e1.printStackTrace();
                    }
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        countDownLatch.countDown();
    }

    private void traverseElement(Iterator<Element> it, CSVWriter csvWriter) {
        while (it.hasNext()) {
            Element book = it.next();
            Iterator itt = book.elementIterator();

            while (itt.hasNext()) {
                ArrayList<String> list = new ArrayList<>();
                Element bookChild = (Element) itt.next();

                for (String field : fields) {
                    Iterator<Element> ittt = bookChild.elementIterator();
                    String value = "";
                    while (ittt.hasNext()) {
                        Element item = ittt.next();
                        if (item.attributeValue("name").equals(field)) {
                            value = item.getStringValue();
                        }
                    }
                    list.add(value);
                }
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                list.add(dateFormat.format(new Date()));
                String[] out = list.toArray(new String[list.size()]);
                csvWriter.writeNext(out);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {

        String fileType = "hw";

        String filepath = "E:\\develop\\Java\\xml2csv\\src\\test\\java\\hw\\";
        String outpath = "E:\\develop\\Java\\xml2csv\\src\\test\\java\\src\\";

        String[] fields;
        if ("hw".equals(fileType)) {
            // hw类型文件的字段名
            fields = new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width", "class"};

        } else if ("jj".equals(fileType)) {
            // jj类型文件的字段名
            fields = new String[] {"sepal_length", "sepal_width", "petal_length", "petal_width", "class", "date"};
        } else {
            throw new IllegalArgumentException("输入参数类型不合法！类型必须是 hw 或 jj ！");
        }

        File file = new File(filepath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            System.out.println("Start to convert kakou xml!");
            for (int i = 0; i < files.length; i++) {
                Xml2CsvMultiThread xml2Csv = new Xml2CsvMultiThread();
                xml2Csv.setOutpath(outpath);
                xml2Csv.setFields(fields);
                xml2Csv.setFileIndex(i);
                xml2Csv.setFiles(files);
                xml2Csv.start();
            }
        }
//        countDownLatch.await();
        System.out.println("converting xml to csv has finished!");
    }
}
