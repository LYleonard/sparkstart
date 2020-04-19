import com.opencsv.CSVWriter;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

/**
 * @ClassName XmltoCsv
 * @Author LYleonard
 * @Date 2020/4/17 16:07
 * @Description TODO
 * Version 1.0
 **/
public class XmltoCsv {

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            String path = args[0];
            String outpath = args[1];
            String fileType = args[2];

            if ("hw".equals(fileType)) {
                // hw类型文件的字段名
                String[] hwFields = {"sepal_length", "sepal_width", "petal_length", "petal_width", "class"};
                readfile(path, outpath, hwFields);
            } else if ("jj".equals(fileType)) {
                // jj类型文件的字段名
                String[] jjFields = {"sepal_length", "sepal_width", "petal_length", "petal_width", "class", "date"};
                readfile(path, outpath, jjFields);
            }
        } else {
            System.out.println(new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                    + "===> 输入参数不合法！");
            System.out.println("输入参数长度为3，第1个参数：xml文件目录，" +
                    "第二个参数：文件处理完移动的目录，" +
                    "第三个参数：文件处理类型（hw、jj）");
        }
    }

    public static boolean readfile(String filepath, String outpath, String[] fields) throws Exception {
        File file = new File(filepath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            System.out.println("Start to convert kakou xml!");

            SAXReader reader = new SAXReader();
            for (int i = 0; i < files.length; ++i) {
                File eachFile = files[i];
                FileInputStream inputStream = new FileInputStream(eachFile);
                try {
                    Document document = reader.read(inputStream);
                    Element table = document.getRootElement();
                    Iterator<Element> it = table.elementIterator();
                    File csvfile = new File(filepath + "/" +
                            eachFile.getName().substring(0, eachFile.getName().lastIndexOf(".")) + ".csv");
                    Writer writer = new FileWriter(csvfile);
                    CSVWriter csvWriter = new CSVWriter(writer);

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
                    System.out.println("converting progress: " + (new DecimalFormat("00.00"))
                            .format((double) i * 100.0D / (double) files.length));
                    eachFile.renameTo(new File(outpath + eachFile.getName()));
                    csvWriter.close();
                } catch (DocumentException e) {
                    inputStream.close();
                    e.printStackTrace();
                }
            }
            System.out.println("converting xml to csv has finished!");
        }
        return true;
    }
}

