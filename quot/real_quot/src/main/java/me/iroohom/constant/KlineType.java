package me.iroohom.constant;


public enum KlineType {

    //首个交易日字段和K线类型
    DAYK("1", "trade_date"), WEEKK("2", "week_first_txdate"), MONTHK("3", "month_first_txdate"), YEARK("4",
            "year_first_txdate");

    private String type;
    private String firstTxDateType;

    /**
     * K线类型
     *
     * @param type
     * @param firstTxDateType
     */
    private KlineType(String type, String firstTxDateType) {
        this.type = type;
        this.firstTxDateType = firstTxDateType;
    }

    /**
     * 获取K线的类型
     *
     * @return
     */
    public String getType() {
        return type;
    }

    /**
     * 获取首个交易日类型
     *
     * @return
     */
    public String getFirstTxDateType() {
        return firstTxDateType;
    }

}
