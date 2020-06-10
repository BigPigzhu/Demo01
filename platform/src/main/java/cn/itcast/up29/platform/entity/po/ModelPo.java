package cn.itcast.up29.platform.entity.po;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity(name = "tbl_model")
public class ModelPo {
    public static final Integer STATE_APPLY=1;
    public static final Integer STATE_PASSED=2;
    public static final Integer STATE_ENABLE=3;
    public static final Integer STATE_DISABLE=4;

    public static final Integer FREQUENCY_ONCE=1;
    public static final Integer FREQUENCY_DAY=2;
    public static final Integer FREQUENCY_WEEK=3;
    public static final Integer FREQUENCY_MONTH=4;
    public static final Integer FREQUENCY_YEAR=5;


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "tag_id")
    private Long tagId;
    @Column(name = "model_name")
    private String name;


    @Column(name = "model_main")
    private String mainClass;
    @Column(name = "model_path")
    private String path;

    @Column(name = "sche_time")
    private String schedule;
    private Date ctime;
    private Date utime;
    private Integer state;//1.申请中,2.审核通过,3.运行中,4.未运行
    private String args;
}
