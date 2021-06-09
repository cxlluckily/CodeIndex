import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.net.ntp.TimeStamp;

import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * @ClassName: DateTest
 * @Author: Roohom
 * @Function:
 * @Date: 2021/3/28 01:16
 * @Software: IntelliJ IDEA
 */
public class DateTest {
    public static void main(String[] args) {
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS",
                TimeZone.getTimeZone("Asia/Shanghai"),
                Locale.CHINA);
        long time = System.currentTimeMillis();
        System.out.println(time);
        String now = fastDateFormat.format(time);
        System.out.println(now);
    }
}
