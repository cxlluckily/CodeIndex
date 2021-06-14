import me.iroohom.Utils.date.DateUtil;
import org.junit.Test;

import java.util.Date;

public class DateTest {

    @Test
    public void DateTestString(){
        System.out.println(DateUtil.getNow());
        System.out.println(DateUtil.getStart());

        System.out.println(DateUtil.getCurrentFormatDate("yyyy-MM-dd HH"));
        System.out.println(new Date(System.currentTimeMillis()));
        System.out.println(DateUtil.getBeforeDate(new Date(System.currentTimeMillis()), 3));
    }



}
