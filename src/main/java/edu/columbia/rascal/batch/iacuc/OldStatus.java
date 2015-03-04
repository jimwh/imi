package edu.columbia.rascal.batch.iacuc;

import java.util.Date;

public class OldStatus {

    public final String statusId;
    public final String statusCode;
    public final Date statusCodeDate;
    public final String userId;
    public final String protocolId;
    public final String statusNote;
    public final String notificationId; // old IACUC Correspondence OID
    public final String snapshotId;

    public OldStatus(
            String statusId,
            String statusCode,
            Date statusCodeDate,
            String userId,
            String protocolId,
            String statusNote,
            String notificationId,
            String snapshotId) {
        this.statusId=statusId;
        this.statusCode=statusCode;
        this.statusCodeDate=statusCodeDate;
        this.userId=userId;
        this.protocolId=protocolId;
        this.statusNote=statusNote;
        this.notificationId=notificationId;
        this.snapshotId=snapshotId;
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[statusId=").append(this.statusId)
                .append(",statusCode=").append(this.statusCode)
                .append(",statusCodeDate=").append(this.statusCodeDate)
                .append(",userId=").append(this.userId)
                .append(",protocolId=").append(this.protocolId)
                .append(",notificationId=").append(this.notificationId)
                .append(",snapshotId=").append(this.snapshotId).append("]");
        return sb.toString();
    }
}
