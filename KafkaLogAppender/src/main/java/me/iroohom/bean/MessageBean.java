package me.iroohom.bean;

/**
 * @ClassName: MessageBean
 * @Author: Roohom
 * @Function: 日志消息实体
 * @Date: 2021/3/28 02:11
 * @Software: IntelliJ IDEA
 */
public class MessageBean {
    String threadName;
    String level;
    String time;
    String loggerName;
    String formattedMessage;

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getLoggerName() {
        return loggerName;
    }

    public void setLoggerName(String loggerName) {
        this.loggerName = loggerName;
    }

    public String getFormattedMessage() {
        return formattedMessage;
    }

    public void setFormattedMessage(String formattedMessage) {
        this.formattedMessage = formattedMessage;
    }

    @Override
    public String toString() {
        return time + " [" + " YarnJob" + "] [" + threadName + "] [" +
                level + "] [" + loggerName + "] " + " - " + formattedMessage;
    }
}
