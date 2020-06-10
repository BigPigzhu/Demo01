package cn.itcast.up29.platform.service;


import cn.itcast.up29.platform.entity.dto.ModelDto;

public interface Engine {

    void startModel(ModelDto modelDto);
    void stopModel(ModelDto modelDto);
}
