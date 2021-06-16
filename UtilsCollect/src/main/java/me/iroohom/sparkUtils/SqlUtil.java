package me.iroohom.sparkUtils;

/**
 * @Author wmh
 * @Date 2020/11/6
 */
public class SqlUtil {
    /**
     * @param filed Character sequence
     * @return sql
     */
    public static String sqlBuilder(String... filed) {
        return String.join("\n", filed);
    }

    /**
     *
     * @param lowerCase  min filter clause
     * @param upperCase  max filter clause
     * @param field     which column for filter
     * @return sql between lowerCase and upperCase characters
     */
    public static String Between(String lowerCase, String upperCase, String field) {
        return " '" + lowerCase + "'<=" + field + " and " + field + "<'" + upperCase + "' ";
    }

    /**
     *
     * @param alias The alias of the table.
     * @param start start time.
     * @param end end time.
     * @return a sql format string.
     */
    public static String totalOrIncr(String alias,String start ,String end) {
        return " (" + alias + ".update_time >= '" + start + "' AND " + alias + ".update_time < '" + end + "')" + " OR " +
                " (" + alias + ".create_time >= '" + start + "' AND " + alias + ".create_time < '"  + end + "')" ;
    }


    /**
     *
     * @param start start time.
     * @param end end time.
     * @return a sql format string.
     */
    public static String totalOrIncr(String start ,String end) {
        return " ("  + "update_time >= '" + start + "' AND "  + "update_time < '" + end + "')" + " OR " +
                " (" + "create_time >= '" + start + "' AND "  + "create_time < '" + end + "')" ;
    }
}
