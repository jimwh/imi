package edu.columbia.rascal.batch.iacuc;

import java.util.Date;

public class AttachedAppendix {

    public final String protocolId;
    public final String appendixType;
    public final String approvalType;
    public final Date approvalDate;

    public AttachedAppendix(String protocolId, String appendixType, String approvalType, Date approvalDate) {
        this.protocolId=protocolId;
        this.appendixType=appendixType;
        this.approvalType=approvalType;
        this.approvalDate=approvalDate;
    }
}
