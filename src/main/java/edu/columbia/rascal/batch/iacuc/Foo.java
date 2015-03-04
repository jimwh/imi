package edu.columbia.rascal.batch.iacuc;

import edu.columbia.rascal.business.service.Migrator;
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

    private static final String SQL_TABLE_MIGRATOR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_MIGRATOR'";
    private static final String SQL_TABLE_IMI = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_IMI'";
    private static final String SQL_TABLE_CORR = "select count(1) from ALL_TABLES where TABLE_NAME='IACUC_CORR'";

    private static final String SQL_CREATE_MIGRATOR = "CREATE table IACUC_MIGRATOR (TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_CORR = "CREATE table IACUC_CORR(TASKID_ NVARCHAR2(64) NOT NULL," +
            "STATUSID_ NVARCHAR2(10) NOT NULL, DATE_ TIMESTAMP(6) NOT NULL)";

    private static final String SQL_CREATE_IMI = "CREATE table IACUC_IMI(POID_ NVARCHAR2(10) NOT NULL, STATUSID_ NVARCHAR2(10) NOT NULL)";

    private static final String SQL_DROP_MIGRATOR = "drop table IACUC_MIGRATOR purge";

    private static final String SQL_DROP_IMI = "drop table IACUC_IMI purge";

    private static final String SQL_UPDATE_MIGRATOR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_MIGRATOR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_MIGRATOR m where a.ID_=m.TASKID_)";

    private static final String SQL_UPDATE_CORR = "update ACT_HI_TASKINST a" +
            " set (a.START_TIME_, a.END_TIME_, a.CLAIM_TIME_)=" +
            " (select m.DATE_, m.DATE_, m.DATE_ from IACUC_CORR m where a.ID_=m.TASKID_)" +
            " where exists ( select 1 from IACUC_CORR m where a.ID_=m.TASKID_)";

    private static final String SQL_OLD_STATUS = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE not in ('Create', 'Release', 'UnRelease', 'Notify', 'Reject', 'PreApprove', " +
            " 'ChgApprovalDate','ChgEffectivDate','ChgEndDate','ChgMeetingDate','FullReviewReq'," +
            " 'HazardsApprove','SOPreApproveA','SOPreApproveB','SOPreApproveC','SOPreApproveD','SOPreApproveE','SOPreApproveF','SOPreApproveG','SOPreApproveI'," +
            " 'SOHoldA','SOHoldB','SOHoldC','SOHoldD','SOHoldE','SOHoldF','SOHoldG','SOHoldI'," +
            " 'VetPreApproveB', 'VetPreApproveC','VetPreApproveE','VetPreApproveF','VetHoldB','VetHoldC','VetHoldE','VetHoldF') " +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_) )" +
            " order by STATUSCODEDATE";

    private static final String SQL_KAPUT_STATUS_1 = "select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID" +
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE<>'Create'" +
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_))" +
            " order by STATUSCODEDATE";

    private static final String SQL_KAPUT_STATUS_2="select OID, STATUSCODE, STATUSCODEDATE, USER_ID, IACUCPROTOCOLHEADERPER_OID, STATUSNOTES, NOTIFICATIONOID, ID" +
            " from IacucProtocolStatus s, RASCAL_USER u, IACUCPROTOCOLSNAPSHOT n" +
            " where s.STATUSSETBY=u.RID"+
            " and s.IACUCPROTOCOLHEADERPER_OID=?" +
            " and s.STATUSCODE not in ('Create', 'Submit', 'Distribute', 'ReturnToPI', 'Approve', 'Done'," +
            " 'Suspend','Terminate','Withdraw','Reinstate', 'ACCMemberHold', 'ACCMemberApprov') "+
            " and s.OID=n.IACUCPROTOCOLSTATUSID(+)" +
            " and s.OID not in (select STATUSID_ from IACUC_MIGRATOR where s.OID=to_number(STATUSID_))" +
            " order by STATUSCODEDATE";

    private static final String SQL_PROTOCOL_ID = "select distinct IACUCPROTOCOLHEADERPER_OID" +
            " from IACUCPROTOCOLSTATUS where STATUSCODE <> 'Create' order by IACUCPROTOCOLHEADERPER_OID";

    private static final String SQL_POID ="select distinct IACUCPROTOCOLHEADERPER_OID"+
            " from IACUCPROTOCOLSTATUS s"+
            " where EXISTS (select 1 from IACUC_IMI imi where s.OID= to_number(imi.statusid_) )"+
            " order by IACUCPROTOCOLHEADERPER_OID";

    private static final String SQL_ALLCORR = "select OID, IACUCPROTOCOLHEADERPER_OID protocolId, USER_ID, CREATIONDATE, RECIPIENTS, CARBONCOPIES, SUBJECT, CORRESPONDENCETEXT" +
            " from IacucCorrespondence c, RASCAL_USER u where c.AUTHORRID=u.RID";

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
    }

    private final JdbcTemplate jdbcTemplate;
    @Resource
    private Migrator migrator;

    @Autowired
    public Foo(JdbcTemplate jt) {
        this.jdbcTemplate = jt;
    }

    public void test() {
        //List<AttachedAppendix> list = migrator.getAttachedAppendix("5050");
        List<AttachedAppendix> list = migrator.getAttachedAppendix("0000");
        log.info("get appendix info...");
        for(AttachedAppendix a: list) {
            log.info("appendixType={},approveType={}, approvalDate={}", a.appendixType, a.approvalType, a.approvalDate);
        }
        log.info("done...");
    }

    public void startMigration() {
       log.info("start up test...");
        migrator.abortProcess("92300", "testing");
        //
        setupTables();
        List<String> plist=new ArrayList<String>();
        // plist.add("90909");
        // plist.add("2967");
        // plist.add("2975");
        // plist.add("33");
        plist.add("92300");
        log.info("testing for plist=" + plist);
        //
        log.info("testing step 1...");
        phase2(plist);
        // get status in progress
        log.info("testing step 2...");
        phase3(plist);
        //
        updateMigrationTables();
        log.info("done...");
    }

    public void startup() {
        log.info("phase 1 set up tables ...");
        setupTables();
        //
        updateMigrationTables();
        //
        List<String> listProtocolId = getListProtocolId(SQL_PROTOCOL_ID);
        log.info("phase 2 import kaput for "+listProtocolId.size() + " protocols");
        List<String> list=new ArrayList<String>();
        for(int i=16000; i<listProtocolId.size(); i++) {
            list.add( listProtocolId.get(i));
        }
        //phase2(listProtocolId);
        phase2(list);
        //
        log.info("phase 3 import corr...");
        importCorr();
        //
        log.info("phase 4 import status...");
        List<String> listPoid = getListProtocolId(SQL_POID);
        phase3(listPoid);
        //
        log.info("phase 5 update migration tables...");
        updateMigrationTables();
        log.info("done...");
    }

    private void importCorr() {
        List<CorrRcd> corrList = getAllCorr();
        log.info("corrList.size="+corrList.size());
        for (CorrRcd corr : corrList) {
            migrator.importCorrRcd(corr);
            // dispatch(corr);
        }
    }

    private void phase3(List<String> listPoid) {
        for (String protocolId : listPoid) {
            // log.info("kaput2: " + SQL_KAPUT_STATUS_2);
            List<OldStatus> list1 = getOldStatusByProtocolId(protocolId, SQL_KAPUT_STATUS_2);
            //for(OldStatus s: list1) {log.info("kaput2: " + s);}
            migrator.importKaput(list1);

            //log.info("oldStatus: " + SQL_OLD_STATUS);
            List<OldStatus> list2 = getOldStatusByProtocolId(protocolId, SQL_OLD_STATUS);
            //for(OldStatus s: list2) {log.info("oldStatus: " + s);}
            migrator.migration(list2);
        }
    }

    private void phase2(List<String> listProtocolId) {
        for (String protocolId : listProtocolId) {
            List<OldStatus> list = getOldStatusByProtocolId(protocolId, SQL_KAPUT_STATUS_1);
            if(list==null) continue;
            if(list.isEmpty()) continue;

            if (!EndSet.contains(list.get(list.size() - 1).statusCode) && list.size() != 1) {
                int last2ndIndex=list.size() - 2;
                if(last2ndIndex>0) {
                    if("Done".equals(list.get(last2ndIndex).statusCode) ) {
                        migrator.importKaput(list);
                        continue;
                    }
                }
                while (true) {
                    int index=list.size()-1;
                    if(index < 0) break;
                    if ("Submit".equals(list.get(index).statusCode)) {
                        OldStatus rcd=list.remove(index);
                        // save protocolId and statusId for next round
                        migrator.insertToImiTable(rcd.protocolId,rcd.statusId);
                        break;
                    } else {
                        list.remove(index);
                    }
                }
            }
            if(list.isEmpty()) continue;
            migrator.importKaput(list);
            // dispatch(list);
        }
    }

    /*
    private void shutdownExcutor() {
        executor.shutdown();
    }
    private void dispatch(List<OldStatus> list) {
        //for(OldStatus s: list) { log.info("before dispatch: " + s); }
        Runnable worker = new KaputWorker(migrator, list);
        executor.execute(worker);
    }
    private void dispatch(CorrRcd corrRcd) {
        Runnable worker = new CorrWorker(migrator, corrRcd);
        executor.execute(worker);
    }
    */

    private void updateMigrationTables() {
        try {
            jdbcTemplate.execute(SQL_UPDATE_MIGRATOR);
            jdbcTemplate.execute(SQL_UPDATE_CORR);
        }catch (Exception e) {
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

    public List<String> getListProtocolId(String sql) {
        RowMapper<String> mapper = new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getString("IACUCPROTOCOLHEADERPER_OID");
            }
        };
        return this.jdbcTemplate.query(sql, mapper);
    }

    private boolean hasMigratorTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_MIGRATOR, Integer.class);
        return one == 1;
    }
    private boolean hasCorrTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_CORR, Integer.class);
        return one == 1;
    }

    private boolean hasImiTable() {
        int one = jdbcTemplate.queryForObject(SQL_TABLE_IMI, Integer.class);
        return one == 1;
    }

    private void createMigratorTable() { jdbcTemplate.execute(SQL_CREATE_MIGRATOR); }
    private void createCorrTable() {
        jdbcTemplate.execute(SQL_CREATE_CORR);
    }
    private void createImiTable() {
        jdbcTemplate.execute(SQL_CREATE_IMI);
    }

    private void dropMigratorTable() {
        jdbcTemplate.execute(SQL_DROP_MIGRATOR);
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

    private void setupTables() {
        if (!hasMigratorTable()) {
            log.info("creating migration table ...");
            createMigratorTable();
        }
        if (!hasCorrTable()) {
            log.info("creating corr table ...");
            createCorrTable();
        }
        if( !hasImiTable()) {
            log.info("creating imi table ...");
            createImiTable();
        }
    }

}
