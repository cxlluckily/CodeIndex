package me.iroohom.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.net.ntp.TimeStamp;

/**
 * @ClassName: Message
 * @Author: Roohom
 * @Function:
 * @Date: 2020/11/4 10:37
 * @Software: IntelliJ IDEA
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message {
    private String userId;
    private String msg;
    private Long eventTime;

}
