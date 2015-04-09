package edu.columbia.rascal.batch.iacuc;

import java.util.Date;

public class OldAdverseStatus {
    public final String adverseStatusId;
    public final String statusCode;
    public final Date statusCodeDate;
    public final String userId;
    public final String adverseId;
    public final String statusNote;
    public final String notificationId;
    public final String snapshotId;

    public OldAdverseStatus(
            String adverseStatusId,
            String statusCode,
            Date statusCodeDate,
            String userId,
            String adverseId,
            String statusNote,
            String notificationId,
            String snapshotId) {
        this.adverseStatusId=adverseStatusId;
        this.statusCode=statusCode;
        this.statusCodeDate=statusCodeDate;
        this.userId=userId;
        this.adverseId=adverseId;
        this.statusNote=statusNote;
        this.notificationId=notificationId;
        this.snapshotId=snapshotId;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[adverseStatusId=").append(adverseStatusId)
        .append(",statusCode=").append(statusCode)
        .append(",statusCodeDate=").append(statusCodeDate)
        .append(",userId=").append(userId)
        .append(",adverseId=").append(adverseId)
        .append(",notificationId=").append(notificationId)
        .append(",snapshotId=").append(snapshotId).append("]");
        return sb.toString();
    }

}

/*******************
 adverse status
select OID, STATUSCODE, STATUSCODEDATE, USER_ID, s.IACUCADVERSEEVENT_OID, STATUSNOTES, NOTIFICATIONOID, ID
        from IACUCADVERSEEVENTSTATUS s, RASCAL_USER u, IACUCADVERSEEVENTSNAPSHOT n
        where s.STATUSSETBY=u.RID
        and s.IACUCADVERSEEVENT_OID=1544
        and s.STATUSCODE<>'Create'
        and s.OID=n.IACUCADVERSEEVENTSTATUSID(+)
        order by STATUSCODEDATE;

 and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_))



 kaput adverse event id
        select distinct IACUCADVERSEEVENT_OID from IacucAdverseEventStatus where STATUSCODE<>'Create'
        and IACUCADVERSEEVENT_OID not in (
        select S.IACUCADVERSEEVENT_OID
        from IacucAdverseEventStatus s
        where s.STATUSCODE not in ('Create', 'Done', 'ReturnToPI', 'Withdraw','Approve', 'ChgMeetingDate',
        'ClosedNoFurther', 'FurtherActionReturnToPI','ACCMemberApprov')
        and s.OID = (select max(st.OID) from IacucAdverseEventStatus st where st.IACUCADVERSEEVENT_OID=s.IACUCADVERSEEVENT_OID)
        )
        order by IACUCADVERSEEVENT_OID;


 in progress adverse event header oid
        select S.IACUCADVERSEEVENT_OID
        from IacucAdverseEventStatus S
        where S.STATUSCODE not in ('Create', 'Done', 'ReturnToPI', 'Withdraw','Approve', 'ChgMeetingDate',
        'ClosedNoFurther', 'FurtherActionReturnToPI', 'ACCMemberApprov')
        and S.OID = (select max(ST.OID) from IacucAdverseEventStatus ST where ST.IACUCADVERSEEVENT_OID=S.IACUCADVERSEEVENT_OID)
        order by S.IACUCADVERSEEVENT_OID;
*****************/