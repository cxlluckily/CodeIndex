package com.csvw.skoda;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Repair {
    /**
     * vin
     */
    private String vin;
    /**
     * 车型
     */
    private String series_code;
    /**
     * 零件名
     */
    private String part_name;

    /**
     * 进站日期
     */
    private String report_date;

    /**
     * 进站里程
     */
    private String current_milemetre;
}
