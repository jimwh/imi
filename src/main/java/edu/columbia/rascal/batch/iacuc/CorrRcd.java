package edu.columbia.rascal.batch.iacuc;

import java.util.Date;

public class CorrRcd {
    public final String oid;
    public final String protocolId;
    public final Date creationDate;
    public final String fromUserId;
    public final String to;
    public final String cc;
    public final String subject;
    public final String body;

    public CorrRcd(
            String oid,
            String protocolId,
            String fromUserId,
            Date creationDate,
            String recipients,
            String cc,
            String subject,
            String text) {

        this.oid=oid;
        this.protocolId=protocolId;
        this.creationDate=creationDate;
        this.fromUserId=fromUserId;
        this.to=recipients;
        this.cc=cc;
        this.subject=subject;
        this.body=text;
    }
}
