package edu.columbia.rascal.business.service.review.iacuc;

import java.util.Map;

public interface IacucTaskFormBase {
    Map<String,String> getProperties();
    void setProperties(Map<String, String> map);
    String getTaskDefKey();
    Map<String,Object> getTaskVariables();
    String getBizKey();
}
