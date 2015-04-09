package edu.columbia.rascal.batch.iacuc;

import java.util.Date;

public class AdverseCorr {

    public final String oid;
    public final String adverseId;
    public final String fromUserId;
    public final Date creationDate;
    public final String to;
    public final String cc;
    public final String subject;
    public final String text;

    public AdverseCorr(
            String oid,
            String adverseIdId,
            String fromUserId,
            Date creationDate,
            String recipients,
            String cc,
            String subject,
            String text) {

        this.oid = oid;
        this.adverseId = adverseIdId;
        this.creationDate = creationDate;
        this.fromUserId = fromUserId;
        this.to = recipients;
        this.cc = cc;
        this.subject = subject;
        this.text = text;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[oid=").append(oid)
                .append(",adverseId=").append(adverseId)
                .append(",from=").append(fromUserId)
                .append(",date=").append(creationDate)
                .append(",to=").append(to)
                .append(",cc=").append(cc)
                .append(",subject=").append(subject)
                .append(",text=").append(text).append("]");
        return sb.toString();
    }
}
