package edu.columbia.rascal.batch.iacuc;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ReviewRcd {

    public final String oid;
    public final String statusId;
    public final String reviewType;
    public final String reviewer1;
    public final String reviewer2;
    public final String reviewer3;
    public final Date meetingDate;

    public ReviewRcd(
            String oid,
            String statusId,
            String reviewType,
            String reviewer1,
            String reviewer2,
            String reviewer3,
            Date meetingDate) {
        this.oid=oid;
        this.statusId=statusId;
        this.reviewType=reviewType;
        this.reviewer1=reviewer1;
        this.reviewer2=reviewer2;
        this.reviewer3=reviewer3;
        this.meetingDate=meetingDate;
    }
    public List<String> getReviewerList() {
        List<String> list=new ArrayList<String>();
        if( !StringUtils.isBlank(reviewer1) ) {
            list.add(reviewer1);
        }
        if( !StringUtils.isBlank(reviewer2) ) {
            list.add(reviewer2);
        }
        if( !StringUtils.isBlank(reviewer3) ) {
            list.add(reviewer3);
        }
        return list;
    }
}
