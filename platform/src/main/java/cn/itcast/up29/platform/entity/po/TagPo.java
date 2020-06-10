package cn.itcast.up29.platform.entity.po;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.util.Date;

@Data
@Entity(name = "tbl_basic_tag")
public class TagPo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long pid;
    private Integer level;
    private String name;
    private String rule;

    //创建时间
    private Date ctime;
    //更新时间
    private Date utime;

}
