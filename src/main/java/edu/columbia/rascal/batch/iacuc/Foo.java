package edu.columbia.rascal.batch.iacuc;

import edu.columbia.rascal.business.service.Migrator;
import edu.columbia.rascal.business.service.review.iacuc.IacucTaskForm;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Component
public class Foo {

    // test tables
    private static final String SQL_TABLE_MIGRATOR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_MIGRATOR'";

    private static final String SQL_TABLE_ADVERSE_MIGRATOR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_ADVERSE_MIGRATOR'";

    private static final String SQL_TABLE_IMI = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_IMI'";
    private static final String SQL_TABLE_CORR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_CORR'";
    private static final String SQL_TABLE_NOTE = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_NOTE'";
    private static final String SQL_TABLE_ATTACHED_CORR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_ATTACHED_CORR'";
    private static final String SQL_TABLE_ADVERSE_ATTACHED_CORR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_ADVERSE_ATTACHED_CORR'";

    // create tables
    private static final String SQL_CREATE_MIGRATOR =
            "CREATE table IACUC_MIGRATOR (TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_ADVERSE_MIGRATOR =
            "CREATE table IACUC_ADVERSE_MIGRATOR (TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL," +
            "CONSTRAINT adverse_mi_pk PRIMARY KEY (TASKID_))";

    private static final String SQL_CREATE_CORR = "CREATE table IACUC_CORR(TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";


    private static final String SQL_CREATE_ATTACHED_CORR = "CREATE table IACUC_ATTACHED_CORR(" +
            "STATUSID_ NVARCHAR2(10) NOT NULL, CORRID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_ADVERSE_ATTACHED_CORR = "CREATE table IACUC_ADVERSE_ATTACHED_CORR(" +
            "STATUSID_ NVARCHAR2(10) NOT NULL, CORRID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";


    private static final String SQL_CREATE_NOTE = "CREATE table IACUC_NOTE(TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_IMI =
            "CREATE table IACUC_IMI(POID_ NVARCHAR2(10) NOT NULL, STATUSID_ NVARCHAR2(10) NOT NULL,"+
            "CONSTRAINT imi_pk PRIMARY KEY (POID_))";

    // drop tables
    private static final String SQL_DROP_MIGRATOR = "drop table IACUC_MIGRATOR purge";
    private static final String SQL_DROP_IMI = "drop table IACUC_IMI purge";
    private static final String SQL_DROP_NOTE = "drop table IACUC_NOTE purge";
    private static final String SQL_DROP_CORR = "drop table IACUC_CORR purge";
    private static final String SQL_DROP_ATTACHED_CORR = "drop table IACUC_ATTACHED_CORR purge";
    private static final String SQL_DROP_ADVERSE_ATTACHED_CORR = "drop table IACUC_ADVERSE_ATTACHED_CORR purge";

    // update table ACT_HI_TASKINST
    private static final String SQL_UPDATE_MIGRATOR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_MIGRATOR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_MIGRATOR m where a.ID_=m.TASKID_)";

    private static final String SQL_UPDATE_ADVERSE_MIGRATOR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_ADVERSE_MIGRATOR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_ADVERSE_MIGRATOR m where a.ID_=m.TASKID_)";

    private static final String SQL_UPDATE_CORR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_CORR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_CORR m where a.ID_=m.TASKID_)";

    private static final String SQL_UPDATE_NOTE = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_NOTE m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_NOTE m where a.ID_=m.TASKID_)";

    // select statement
    private static final String SQL_KAPUT_PROTOCOL_ID = "select distinct IACUCPROTOCOLHEADERPER_OID" +
            " from IACUCPROTOCOLSTATUS where STATUSCODE <> 'Create'" +
            " and IACUCPROTOCOLHEADERPER_OID not in (" +
            " select s.IACUCPROTOCOLHEADERPER_OID" +
            " from IacucProtocolStatus s" +
            " where s.STATUSCODE not in ('Create', 'Done', 'ReturnToPI', 'Terminate', 'Withdraw'," +
            " 'Suspend', 'Approve', 'Notify', 'ChgApprovalDate', 'ChgEffectivDate')" +
            " and s.OID = (select max(st.OID) from IacucProtocolStatus st where st.IACUCPROTOCOLHEADERPER_OID=s.IACUCPROTOCOLHEADERPER_OID))" +
            " and OID not in(select STATUSID_ from IACUC_MIGRATOR)" +
            "order by IACUCPROTOCOLHEADERPER_OID";

    private static final String SQL_IN_PROGRESS_PROTOCOL_HEADER_OID =
            "select IACUCPROTOCOLHEADERPER_OID from IacucProtocolStatus S" +
                    " where S.STATUSCODE not in ('Create', 'Done', 'ReturnToPI', 'Terminate', 'Withdraw', 'Suspend', 'Approve', 'Notify', 'ChgApprovalDate', 'ChgEffectivDate')" +
                    " and S.OID = (select max(st.OID) from IacucProtocolStatus st where st.IACUCPROTOCOLHEADERPER_OID=S.IACUCPROTOCOLHEADERPER_OID)" +
                    " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR)" +
                    " order by IACUCPROTOCOLHEADERPER_OID";

    /*
    private static final String SQL_OLD_STATUS = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE not in ('Create', 'Release', 'UnRelease', 'Notify', 'Reject', 'PreApprove', " +
            " 'ChgApprovalDate','ChgEffectivDate','ChgEndDate','ChgMeetingDate'," +
            " 'HazardsApprove'," +
            " 'VetPreApproveB', 'VetPreApproveC','VetPreApproveE','VetPreApproveF','VetHoldB','VetHoldC','VetHoldE','VetHoldF') " +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_) )" +
            " order by STATUSCODEDATE";
    */

    private static final String SQL_KAPUT_STATUS_1 = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE<>'Create'" +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_))" +
            " order by STATUSCODEDATE";


    /*
    private static final String SQL_PROTOCOL_ID = "select distinct IACUCPROTOCOLHEADERPER_OID" +
            " from IACUCPROTOCOLSTATUS where STATUSCODE <> 'Create' order by IACUCPROTOCOLHEADERPER_OID";

    private static final String SQL_POID = "select distinct IACUCPROTOCOLHEADERPER_OID" +
            " from IACUCPROTOCOLSTATUS s" +
            " where EXISTS (select 1 from IACUC_IMI imi where s.OID= to_number(imi.statusid_) )" +
            " order by IACUCPROTOCOLHEADERPER_OID";
    */

    // IACUC_CORR table STATUSID_ <-- CORR OID
    // IACUC_ATTACHED_CORR table CORRID_ <-- CORR OID
    private static final String SQL_ALLCORR =
            "select OID, IACUCPROTOCOLHEADERPER_OID protocolId, USER_ID, CREATIONDATE, RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT" +
                    " from IacucCorrespondence c, RASCAL_USER u where c.AUTHORRID=u.RID" +
                    " and SUBJECT is not null" +
                    " and length(trim(RECIPIENTS)) > 0" +
                    //" and CORRESPONDENCETEXT is not null" +
                    //" and length(trim(CORRESPONDENCETEXT)) > 0" +
                    " and OID not in (select STATUSID_ from IACUC_CORR)" +
                    " and OID not in (select CORRID_ from IACUC_ATTACHED_CORR) order by OID";

    private static final String SQL_ALL_ADVERSE_EVT_CORR =
            "select OID, IACUCADVERSEEVENT_OID, USER_ID, CREATIONDATE,"+
            "RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT"+
            " from IACUCADVERSEEVENTCORRESPOND c, RASCAL_USER u"+
            " where c.AUTHORRID=u.RID"+
            " and SUBJECT is not null"+
            " and length(trim(RECIPIENTS)) > 0"+
            " and OID not in (select STATUSID_ from IACUC_ADVERSE_ATTACHED_CORR)"+
            " and OID not in (select CORRID_ from IACUC_ADVERSE_ATTACHED_CORR)"+
            " order by OID";

    private static final String SQL_OLD_NOTE =
            "select OID, N.IACUCPROTOCOLHEADERPER_OID, U.USER_ID, N.NOTETEXT, N.LASTMODIFICATIONDATE" +
                    " from IACUCPROTOCOLNOTES N, RASCAL_USER U" +
                    " where N.NOTEAUTHOR is not null and U.RID=N.NOTEAUTHOR" +
                    " order by N.IACUCPROTOCOLHEADERPER_OID";


    private static final String SQL_ADVERSE_KAPUT_EVT_OID =
            "select distinct IACUCADVERSEEVENT_OID from IacucAdverseEventStatus where STATUSCODE<>'Create'" +
                    " and IACUCADVERSEEVENT_OID not in(" +
                    " select SS.IACUCADVERSEEVENT_OID" +
                    " from IacucAdverseEventStatus SS" +
                    " where SS.STATUSCODE not in ('Create', 'ACCMemberApprov', 'Done', 'ReturnToPI', 'Withdraw','Approve', 'ChgMeetingDate', 'ClosedNoFurther', 'FurtherActionReturnToPI')" +
                    " and SS.OID = (select max(ST.OID) from IacucAdverseEventStatus ST where ST.IACUCADVERSEEVENT_OID=SS.IACUCADVERSEEVENT_OID)" +
                    " ) order by IACUCADVERSEEVENT_OID";


    private static final String SQL_IN_PROGRESS_ADVERSE_ID =
            "select S.IACUCADVERSEEVENT_OID from IacucAdverseEventStatus S" +
                    " where S.STATUSCODE not " +
                    " in ('Create', 'ACCMemberApprov', 'Done', 'ReturnToPI', 'Withdraw','Approve', 'ChgMeetingDate', 'ClosedNoFurther', 'FurtherActionReturnToPI')" +
                    " and S.OID = (select max(ST.OID) from IacucAdverseEventStatus ST where ST.IACUCADVERSEEVENT_OID=S.IACUCADVERSEEVENT_OID)" +
                    " order by S.IACUCADVERSEEVENT_OID";

    private static final String SQL_ADVERSE_STATUS =
            "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, s.IACUCADVERSEEVENT_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
                    " from IACUCADVERSEEVENTSTATUS s, RASCAL_USER u, IACUCADVERSEEVENTSNAPSHOT n" +
                    " where s.STATUSSETBY=u.RID" +
                    " and s.IACUCADVERSEEVENT_OID=?" +
                    " and s.STATUSCODE<>'Create'" +
                    " and s.OID=n.IACUCADVERSEEVENTSTATUSID(+)" +
                    " order by STATUSCODEDATE";

    private static final Logger log = LoggerFactory.getLogger(Foo.class);

    private static final Set<String> EndSet = new HashSet<String>();

    static {
        EndSet.add("Withdraw");
        EndSet.add("ReturnToPI");
        EndSet.add("Approve");
        EndSet.add("Done");
        EndSet.add("Suspend");
        EndSet.add("Terminate");
        EndSet.add("Reinstate");
        EndSet.add("Notify");
        EndSet.add("ChgApprovalDate");
        EndSet.add("ChgEffectivDate");
        EndSet.add("ChgEndDate");
        EndSet.add("ChgMeetingDate");
        EndSet.add("HazardsApprove");
        EndSet.add("PreApprove");
        EndSet.add("Reject");
        EndSet.add("Release");
        EndSet.add("UnRelease");
        EndSet.add("VetHoldB");
        EndSet.add("VetHoldC");
        EndSet.add("VetHoldE");
        EndSet.add("VetHoldF");
        EndSet.add("VetPreApproveB");
        EndSet.add("VetPreApproveC");
        EndSet.add("VetPreApproveE");
        EndSet.add("VetPreApproveF");
    }

    private static final Set<String> InProgressHeaderOid = new HashSet<String>();
    private static final Set<String> AdverseEndSet = new HashSet<String>();

    static {
        AdverseEndSet.add("ACCMemberApprov");
        AdverseEndSet.add("Done");
        AdverseEndSet.add("ReturnToPI");
        AdverseEndSet.add("Withdraw");
        AdverseEndSet.add("Approve");
        AdverseEndSet.add("ChgMeetingDate");
        AdverseEndSet.add("ClosedNoFurther");
        AdverseEndSet.add("FurtherActionReturnToPI");
    }

    private final JdbcTemplate jdbcTemplate;
    @Resource
    private Migrator migrator;

    @Autowired
    public Foo(JdbcTemplate jt) {
        this.jdbcTemplate = jt;
    }


    public void testAdverse() {

        int notificationIdCount = 0;
        List<String> adverseIdKaputList = getAdverseIdList(SQL_ADVERSE_KAPUT_EVT_OID);
        log.info("kaput IACUCADVERSEEVENT_OID size={}", adverseIdKaputList.size());
        for (String id : adverseIdKaputList) {
            List<OldAdverseStatus> oldAdverseStatusList = getOldAdverseStatusByAdverseId(id, SQL_ADVERSE_STATUS);
            log.info("status size={}", oldAdverseStatusList.size());
            for (OldAdverseStatus oldAdverseStatus : oldAdverseStatusList) {
                log.info("adverseStatus={}", oldAdverseStatus);
                if (!StringUtils.isBlank(oldAdverseStatus.notificationId)) {
                    notificationIdCount += 1;
                    log.info("notificationId={}", oldAdverseStatus.notificationId);
                    AdverseCorr adverseCorr = migrator.getAdverseCorrRcdByNotificationId(oldAdverseStatus.notificationId);
                    log.info("adverseCorr={}", adverseCorr);
                }
            }
        }

        //
        List<String> adverseIdInprogressList = getAdverseIdList(SQL_IN_PROGRESS_ADVERSE_ID);
        log.info("in progress IACUCADVERSEEVENT_OID size={}", adverseIdInprogressList.size());
        for (String id : adverseIdInprogressList) {
            List<OldAdverseStatus> oldAdverseStatusList = getOldAdverseStatusByAdverseId(id, SQL_ADVERSE_STATUS);
            log.info("status size={}", oldAdverseStatusList.size());
            for (OldAdverseStatus oldAdverseStatus : oldAdverseStatusList) {
                log.info("adverseStatus={}", oldAdverseStatus);
                if (!StringUtils.isBlank(oldAdverseStatus.notificationId)) {
                    notificationIdCount += 1;
                    log.info("notificationId={}", oldAdverseStatus.notificationId);
                    AdverseCorr adverseCorr = migrator.getAdverseCorrRcdByNotificationId(oldAdverseStatus.notificationId);
                    log.info("adverseCorr={}", adverseCorr);
                }
            }
        }

        log.info("number of notifications={}", notificationIdCount);
    }

    public void testKaputAdverse() {
        setupTables();
        String adverseId = "3";
        List<OldAdverseStatus> oldAdverseStatusList = getOldAdverseStatusByAdverseId(adverseId, SQL_ADVERSE_STATUS);
        log.info("status size={}", oldAdverseStatusList.size());
        migrator.importKaputAdverse(oldAdverseStatusList);
        // jdbcTemplate.execute(SQL_UPDATE_MIGRATOR);
        List<IacucTaskForm> list=migrator.getIacucAdverseHistory(adverseId);
        for(IacucTaskForm form: list) {
            log.info(form.toString());
        }
    }



    public void testTables() {
        setupTables();
        if( !hasImiTable() ) {
            createImiTable();
        }
        migrator.insertToImiTable("1", "1");
    }

    public void test() {
        //List<AttachedAppendix> list = migrator.getAttachedAppendix("5050");
        List<AttachedAppendix> list = migrator.getAttachedAppendix("0000");
        log.info("get appendix info...");
        for (AttachedAppendix a : list) {
            log.info("appendixType={},approveType={}, approvalDate={}", a.appendixType, a.approvalType, a.approvalDate);
        }
        log.info("done...");
    }

    public void testGetNote() {
        log.info("set up tables ...");
        if (!hasNoteTable()) {
            log.info("creating note table ...");
            createNoteTable();
        }
        List<OldNote> list = getAllOldNotes();
        for (OldNote note : list) {
            log.info("author={}, note={}, date={}", note.author, note.note, note.date);
        }
        log.info("import note...");
        importOldNote();
        jdbcTemplate.execute(SQL_UPDATE_NOTE);
    }

    public void testSubset() {
        log.info("test subset of data ...");
        migrator.abortProcess("10530", "testing");
        //
        setupTables();
        //
        List<String> plist = new ArrayList<String>();
        plist.add("10530");
        log.info("testing plist={}", plist.toString());
        walkThrough(plist);
        //
        updateMigrationTables();
        // printHistoryByBizKey(protocolId);
        List<CorrRcd> corrList = getAllCorr();
        log.info("corrSize={}", corrList.size());
        printHistoryByBizKey("10530");
    }

    public void startup() {
        log.info("set up tables ...");
        setupTables();
        // updateMigrationTables();
        //
        setInProgressHeaderOid();
        //
        // first import all kaput status
        List<String> listKaputProtocolId = getListProtocolId(SQL_KAPUT_PROTOCOL_ID);
        log.info("number of kaput protocols: {}", listKaputProtocolId.size());
        importKaputByKaputProtocolId(listKaputProtocolId);
        //
        // next in progress status
        List<String> listProtocolId = getListProtocolId(SQL_IN_PROGRESS_PROTOCOL_HEADER_OID);
        log.info("number of protocols: {}", listProtocolId.size());
        walkThrough(listProtocolId);
        //
        log.info("import corr...");
        importCorr();
        //
        log.info("import note...");
        importOldNote();
        //
        log.info("import adverse event data ...");
        importAdverseEventData();
        //
        log.info("import adverse evt corr...");
        importAdverseEvtCorr();
        //
        log.info("update migration tables...");
        updateMigrationTables();
    }

    private void importKaputByKaputProtocolId(List<String> listProtocolId) {
        for (String protocolId : listProtocolId) {
            List<OldStatus> list = getOldStatusByProtocolId(protocolId, SQL_KAPUT_STATUS_1);
            migrator.importKaput(list);
        }
    }

    private void importCorr() {
        List<CorrRcd> corrList = getAllCorr();
        log.info("corrList.size=" + corrList.size());
        for (CorrRcd corr : corrList) {
            migrator.importCorrRcd(corr);
        }
    }
    private void importAdverseEvtCorr() {
        List<AdverseCorr> corrList = getAllAdverseEvtCorr();
        log.info("corrList.size=" + corrList.size());
        for (AdverseCorr corr : corrList) {
            migrator.importAdverseEvtCorrRcd(corr);
        }
    }

    private void importOldNote() {
        List<OldNote> list = getAllOldNotes();
        log.info("number of old notes: {}", list.size());
        for (OldNote note : list) {
            migrator.importOldNote(note);
        }
    }

    // It may be mixed with finished or unfinished records.
    // Remove unfinished records and insert them into IMI table.
    // Import finished records as kaput status
    private void walkThrough(List<String> listProtocolId) {
        for (String protocolId : listProtocolId) {
            List<OldStatus> list = getOldStatusByProtocolId(protocolId, SQL_KAPUT_STATUS_1);

            if (!InProgressHeaderOid.contains(protocolId)) {
                migrator.importKaput(list);
                continue;
            }

            if (list == null || list.isEmpty()) continue;
            // last element is in the EndSet
            // which means these status had been done already
            int lastIndex = list.size() - 1;
            OldStatus lastStatus = list.get(lastIndex);
            if (EndSet.contains(lastStatus.statusCode)) {
                migrator.importKaput(list);
                continue;
            } else {
                lastIndex -= 1;
                if (lastIndex > -1) {
                    lastStatus = list.get(lastIndex);
                    if (lastStatus != null) {
                        if ("Done".equals(lastStatus.statusCode) || "Approve".equals(lastStatus.statusCode)) {
                            migrator.importKaput(list);
                            continue;
                        }
                    }
                }
            }

            // move from bottom up to Submit status
            Deque<OldStatus> deque = new LinkedList<OldStatus>();

            while (true) {
                int index = list.size() - 1;
                if (index < 0) break;
                if ("Submit".equals(list.get(index).statusCode)) {
                    deque.addFirst(list.get(index));
                    OldStatus rcd = list.remove(index);
                    // save protocolId and statusId for next round
                    // migrator.insertToImiTable(rcd.protocolId, rcd.statusId);
                    break;
                } else {
                    deque.addFirst(list.get(index));
                    list.remove(index);
                }
            }
            if (!list.isEmpty()) {
                migrator.importKaput(list);
            }

            if (!deque.isEmpty()) {
                migrator.migrateReviewInProgress(deque);
            }
        }
    }

    private void updateMigrationTables() {
        try {
            jdbcTemplate.execute(SQL_UPDATE_MIGRATOR);
            jdbcTemplate.execute(SQL_UPDATE_CORR);
            jdbcTemplate.execute(SQL_UPDATE_NOTE);
            jdbcTemplate.execute(SQL_UPDATE_ADVERSE_MIGRATOR);
        } catch (Exception e) {
            log.error("err in update table:", e);
        }
    }

    private List<OldStatus> getOldStatusByProtocolId(String protocolId, String sql) {

        RowMapper<OldStatus> mapper = new RowMapper<OldStatus>() {
            @Override
            public OldStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
                OldStatus rcd = new OldStatus(
                        rs.getString("OID"),
                        rs.getString("STATUSCODE"),
                        rs.getTimestamp("STATUSCODEDATE"),
                        rs.getString("USER_ID"),
                        rs.getString("IACUCPROTOCOLHEADERPER_OID"),
                        rs.getString("STATUSNOTES"),
                        rs.getString("NOTIFICATIONOID"),
                        rs.getString("ID")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(sql, mapper, protocolId);
    }

    private List<OldAdverseStatus> getOldAdverseStatusByAdverseId(String adverseId, String sql) {

        RowMapper<OldAdverseStatus> mapper = new RowMapper<OldAdverseStatus>() {
            @Override
            public OldAdverseStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
                OldAdverseStatus rcd = new OldAdverseStatus(
                        rs.getString("OID"),
                        rs.getString("STATUSCODE"),
                        rs.getTimestamp("STATUSCODEDATE"),
                        rs.getString("USER_ID"),
                        rs.getString("IACUCADVERSEEVENT_OID"),
                        rs.getString("STATUSNOTES"),
                        rs.getString("NOTIFICATIONOID"),
                        rs.getString("ID")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(sql, mapper, adverseId);
    }

    private void setInProgressHeaderOid() {
        List<String> list = getListProtocolId(SQL_IN_PROGRESS_PROTOCOL_HEADER_OID);
        log.info("in progress size={}", list.size());
        InProgressHeaderOid.addAll(list);
    }

    public List<String> getListProtocolId(String sql) {
        RowMapper<String> mapper = new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getString("IACUCPROTOCOLHEADERPER_OID");
            }
        };
        return this.jdbcTemplate.query(sql, mapper);
    }

    public List<String> getAdverseIdList(String sql) {
        RowMapper<String> mapper = new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getString("IACUCADVERSEEVENT_OID");
            }
        };
        return this.jdbcTemplate.query(sql, mapper);
    }

    private boolean hasMigratorTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_MIGRATOR, Integer.class);
        return one == 1;
    }

    private boolean hasAdverseMigratorTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_ADVERSE_MIGRATOR, Integer.class);
        return one == 1;
    }

    private boolean hasCorrTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_CORR, Integer.class);
        return one == 1;
    }

    private boolean hasAttachedCorrTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_ATTACHED_CORR, Integer.class);
        return one == 1;
    }

    private boolean hasAdverseAttachedCorrTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_ADVERSE_ATTACHED_CORR, Integer.class);
        return one == 1;
    }

    private boolean hasNoteTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_NOTE, Integer.class);
        return one == 1;
    }

    private boolean hasImiTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_IMI, Integer.class);
        return one == 1;
    }

    private void createMigratorTable() { jdbcTemplate.execute(SQL_CREATE_MIGRATOR); }

    private void createAdverseMigratorTable() { jdbcTemplate.execute(SQL_CREATE_ADVERSE_MIGRATOR); }

    private void createCorrTable() {
        jdbcTemplate.execute(SQL_CREATE_CORR);
    }

    private void createAttachedCorrTable() {
        jdbcTemplate.execute(SQL_CREATE_ATTACHED_CORR);
    }

    private void createAdverseAttachedCorrTable() { jdbcTemplate.execute(SQL_CREATE_ADVERSE_ATTACHED_CORR); }

    private void createImiTable() {
        jdbcTemplate.execute(SQL_CREATE_IMI);
    }

    private void createNoteTable() {
        jdbcTemplate.execute(SQL_CREATE_NOTE);
    }

    private void dropMigratorTable() {
        jdbcTemplate.execute(SQL_DROP_MIGRATOR);
    }

    private void dropCorrTable() {
        jdbcTemplate.execute(SQL_DROP_CORR);
    }

    private void dropNoteTable() {
        jdbcTemplate.execute(SQL_DROP_NOTE);
    }

    public List<CorrRcd> getAllCorr() {
        RowMapper<CorrRcd> mapper = new RowMapper<CorrRcd>() {
            @Override
            public CorrRcd mapRow(ResultSet rs, int rowNum) throws SQLException {
                CorrRcd rcd = new CorrRcd(
                        rs.getString("OID"),
                        rs.getString("protocolId"),
                        rs.getString("USER_ID"),
                        rs.getTimestamp("CREATIONDATE"),
                        rs.getString("RECIPIENTS"),
                        rs.getString("CARBONCOPIES"),
                        rs.getString("SUBJECT"),
                        rs.getString("CORRESPONDENCETEXT")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(SQL_ALLCORR, mapper);
    }

    public List<AdverseCorr> getAllAdverseEvtCorr() {
        RowMapper<AdverseCorr> mapper = new RowMapper<AdverseCorr>() {
            @Override
            public AdverseCorr mapRow(ResultSet rs, int rowNum) throws SQLException {
                AdverseCorr rcd = new AdverseCorr(
                        rs.getString("OID"),
                        rs.getString("IACUCADVERSEEVENT_OID"),
                        rs.getString("USER_ID"),
                        rs.getTimestamp("CREATIONDATE"),
                        rs.getString("RECIPIENTS"),
                        rs.getString("CARBONCOPIES"),
                        rs.getString("SUBJECT"),
                        rs.getString("CORRESPONDENCETEXT")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(SQL_ALL_ADVERSE_EVT_CORR, mapper);
    }

    public List<OldNote> getAllOldNotes() {
        RowMapper<OldNote> mapper = new RowMapper<OldNote>() {
            @Override
            public OldNote mapRow(ResultSet rs, int rowNum) throws SQLException {
                OldNote rcd = new OldNote(
                        rs.getString("OID"),
                        rs.getString("IACUCPROTOCOLHEADERPER_OID"),
                        rs.getString("USER_ID"),
                        rs.getString("NOTETEXT"),
                        rs.getTimestamp("LASTMODIFICATIONDATE")
                );
                return rcd;
            }
        };
        return this.jdbcTemplate.query(SQL_OLD_NOTE, mapper);
    }

    private void setupTables() {
        if (!hasMigratorTable()) {
            log.info("creating IACUC_MIGRATOR table ...");
            createMigratorTable();
        }

        if ( !hasAdverseMigratorTable() ) {
            log.info("creating IACUC_ADVERSE_MIGRATOR table ...");
            createAdverseMigratorTable();
        }

        if (!hasCorrTable()) {
            log.info("creating IACUC_CORR table ...");
            createCorrTable();
        }

        if (!hasAttachedCorrTable()) {
            log.info("creating IACUC_ATTACHED_CORR table ...");
            createAttachedCorrTable();
        }

        if (!hasAdverseAttachedCorrTable()) {
            log.info("creating IACUC_ADVERSE_ATTACHED_CORR table ...");
            createAdverseAttachedCorrTable();
        }

        if (!hasImiTable()) {
            log.info("creating IACUC_IMI table ...");
            createImiTable();
        }

        if (!hasNoteTable()) {
            log.info("creating IACUC_NOTE table ...");
            createNoteTable();
        }

    }

    public void printHistoryByBizKey(String protocolId) {
        List<IacucTaskForm> list = migrator.getIacucProtocolHistory(protocolId);
        for (IacucTaskForm form : list) {
            log.info(form.toString());
        }
    }

    public void printHistoryByBizKey(int protocolId) {
        List<IacucTaskForm> list = migrator.getIacucProtocolHistory(String.valueOf(protocolId));
        for (IacucTaskForm form : list) {
            log.info("taskDefKey={}, taskName={}, endTime={}", form.getTaskDefKey(), form.getTaskName(), form.getEndTimeString());
        }
    }

    void importAdverseEventData() {

        // import closed data
        List<String> adverseIdKaputList = getAdverseIdList(SQL_ADVERSE_KAPUT_EVT_OID);
        for (String adverseId : adverseIdKaputList) {
            List<OldAdverseStatus> list = getOldAdverseStatusByAdverseId(adverseId, SQL_ADVERSE_STATUS);
            migrator.importKaputAdverse(list);
        }

        // import in progress data
        List<String> adverseIdInprogressList = getAdverseIdList(SQL_IN_PROGRESS_ADVERSE_ID);
        for(String adverseEvtId: adverseIdInprogressList) {
            List<OldAdverseStatus> oldAdverseStatusList = getOldAdverseStatusByAdverseId(adverseEvtId, SQL_ADVERSE_STATUS);
            importInProgressAdverse(oldAdverseStatusList);
        }
    }

    private void importInProgressAdverse(List<OldAdverseStatus> list) {
        if(list==null || list.isEmpty()) return;
        log.info("size={}", list.size());
        int lastIndex=list.size()-1;
        OldAdverseStatus status=list.get(lastIndex);
        if( AdverseEndSet.contains(status.statusCode) ) {
            migrator.importKaputAdverse(list);
            return;
        }

        Deque<OldAdverseStatus> deque=new LinkedList<OldAdverseStatus>();
        while( true ) {
            int index=list.size()-1;
            if( index < 0) break;
            if( "Submit".equals(list.get(index).statusCode)) {
                deque.addFirst(list.get(index));
                list.remove(index);
                break;
            }else {
                deque.addFirst(list.get(index));
                list.remove(index);
            }
        }

        if( !list.isEmpty() ) {
            migrator.importKaputAdverse(list);
        }

        if( !deque.isEmpty() ) {
            migrator.migrateAdverseInProgress(deque);
        }
    }



    public void shutdown() {
        migrator.shutdownExcutor();
    }
}
