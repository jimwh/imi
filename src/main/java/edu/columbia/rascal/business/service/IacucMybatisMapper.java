package edu.columbia.rascal.business.service;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface IacucMybatisMapper {

    @Select("SELECT ID_ , NAME_ , CREATE_TIME_ FROM ACT_RU_TASK")
    List<Map<String, Object>> selectTasks();

    @Select({
            "SELECT task.ID_ as taskId, variable.LONG_ as variableValue FROM ACT_RU_VARIABLE variable",
            "inner join ACT_RU_TASK task on variable.TASK_ID_ = task.ID_",
            "where variable.NAME_ = #{variableName}"
    })
    List<Map<String, Object>> selectTaskWithSpecificVariable(String variableName);

    @Update({
            "update ACT_HI_TASKINST set START_TIME_=#{endTime}, END_TIME_=#{endTime}, CLAIM_TIME_=#{endTime} where ID_=#{taskId}"
            })
    void updateEndTime(@Param("taskId") String taskId, @Param("endTime") Date endTime);

}
