package cc.hiifong.bigdata;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;

/**
 * @Author hiifong
 * @Date 2023/6/9 10:55
 * @Email i@hiif.ong
 */
public class CustomFunction extends UDF {
    public Text evaluate(Text subject) {
        return new Text("101\thiifong\t" + subject);
    }
}
