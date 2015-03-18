package edu.columbia.rascal.business.service.review.iacuc;

import java.util.HashSet;
import java.util.Set;

public enum IacucStatus {

    Submit("Submit") {
        @Override
        public String taskDefKey() {
            return "submit";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 1;
        }

        @Override
        public String getCandidateGroup() {
            return "";
        }
    },


    ReturnToPI("Return to PI") {
        @Override
        public String taskDefKey() {
            return "returnToPI";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 55;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_RETURN_TO_PI,IACUC_CAN_RETURN_COLUMBIA_PROTOCOL_TO_PI";
        }
    },

    FinalApproval("Approve") {
        @Override
        public String taskDefKey() {
            return "finalApproval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 66;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_APPROVE_COLUMBIA_PROTOCOL";
        }
    },


    AdverseEvent("AdverseEvent") {
        @Override
        public String taskDefKey() {
            return "adverseEvent";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 3;
        }

        @Override
        public String getCandidateGroup() {
            return "";
        }
    },

    Terminate("Terminate") {
        @Override
        public String taskDefKey() {
            return "terminateProtocol";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 3;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_TERMINATE_COLUMBIA_PROTOCOL";
        }
    },

    Suspend("Suspend") {
        @Override
        public String taskDefKey() {
            return "suspendProtocol";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 4;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_SUSPEND_COLUMBIA_PROTOCOL";
        }
    },

    Withdraw("Withdraw") {
        @Override
        public String taskDefKey() {
            return "withdrawal";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 5;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_WITHDRAW_COLUMBIA_PROTOCOL";
        }
    },

    AddCorrespondence("Add Correspondence") {
        @Override
        public String taskDefKey() {
            return "addCorrespondence";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 6;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_ADD_CORRESPONDENCE";
        }
    },

    Reinstate("Reinstate") {
        @Override
        public String taskDefKey() {
            return "reinstatement";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equalsIgnoreCase(status);
        }

        @Override
        public int gatewayValue() {
            return 7;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REINSTATE_COLUMBIA_PROTOCOL";
        }
    },

    Kaput("Kaput") {
        @Override
        public String taskDefKey() {
            return "kaput";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status) ? true : isKaput(status);
        }

        @Override
        public int gatewayValue() {
            return 8;
        }

        @Override
        public String getCandidateGroup() {
            return "";
        }
    },

    UndoApproval("Undo Approval") {
        @Override
        public String taskDefKey() {
            return "undoApproval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_UNDO_APPROVE_COLUMBIA_PROTOCOL";
        }
    },

    DistributeSubcommittee("Distribute: Subcommittee") {
        @Override
        public String taskDefKey() {
            return "distributeToSub";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 1;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_DISTRIBUTE_SUB_COMMITTEE_PROTOCOL";
        }
    },

    DistributeReviewer("Distribute: Designated Reviewers") {
        @Override
        public String taskDefKey() {
            return "distributeToDS";
        }

        @Override
        public boolean isDefKey(String def) {
            return taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 2;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_DISTRIBUTE_DE_REVIEWER_PROTOCOL";
        }
    },

    Rv1Approval("Designated Reviewer Approval") {
        @Override
        public String taskDefKey() {
            return "rv1Approval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_APPROVE_PROTOCOL";
        }
    },
    Rv1Hold("Designated Reviewer Hold") {
        @Override
        public String taskDefKey() {
            return "rv1Hold";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_HOLD_PROTOCOL";
        }

    },

    Rv1ReqFullReview("Designated Reviewer Request Full Review") {
        @Override
        public String taskDefKey() {
            return "rv1ReqFullReview";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_FULL_BOARD_PROTOCOL";
        }

    },

    Rv2Approval("Designated Reviewer Approval") {
        @Override
        public String taskDefKey() {
            return "rv2Approval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_APPROVE_PROTOCOL";
        }

    },

    Rv2Hold("Designated Reviewer Hold") {
        @Override
        public String taskDefKey() {
            return "rv2Hold";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_HOLD_PROTOCOL";
        }

    },

    Rv2ReqFullReview("Designated Reviewer Request Full Review") {
        @Override
        public String taskDefKey() {
            return "rv2ReqFullReview";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_FULL_BOARD_PROTOCOL";
        }

    },

    Rv3Approval("Designated Reviewer Approval") {
        @Override
        public String taskDefKey() {
            return "rv3Approval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_APPROVE_PROTOCOL";
        }

    },
    Rv3Hold("Designated Reviewer Hold") {
        @Override
        public String taskDefKey() {
            return "rv3Hold";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_HOLD_PROTOCOL";
        }

    },
    Rv3ReqFullReview("Designated Reviewer Request Full Review") {
        @Override
        public String taskDefKey() {
            return "rv3ReqFullReview";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_FULL_BOARD_PROTOCOL";
        }

    },

    Rv4Approval("Designated Reviewer Approval") {
        @Override
        public String taskDefKey() {
            return "rv4Approval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_APPROVE_PROTOCOL";
        }

    },
    Rv4Hold("Designated Reviewer Hold") {
        @Override
        public String taskDefKey() {
            return "rv4Hold";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_HOLD_PROTOCOL";
        }

    },
    Rv4ReqFullReview("Designated Reviewer Request Full Review") {
        @Override
        public String taskDefKey() {
            return "rv4ReqFullReview";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_FULL_BOARD_PROTOCOL";
        }

    },
    Rv5Approval("Designated Reviewer Approval") {
        @Override
        public String taskDefKey() {
            return "rv5Approval";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_APPROVE_PROTOCOL";
        }

    },
    Rv5Hold("Designated Reviewer Hold") {
        @Override
        public String taskDefKey() {
            return "rv5Hold";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_HOLD_PROTOCOL";
        }

    },
    Rv5ReqFullReview("Designated Reviewer Request Full Review") {
        @Override
        public String taskDefKey() {
            return "rv5ReqFullReview";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            // unused
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_REVIEW_FULL_BOARD_PROTOCOL";
        }

    },

    AddNote("Add Note") {
        @Override
        public String taskDefKey() {
            return "addNote";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_ADD_NOTE_PROTOCOL";
        }

    },


    SOPreApproveA("Safety Office Pre-approve Appendix-A") {
        @Override
        public String taskDefKey() {
            return "soPreApproveA";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_A";
        }
    },

    SOHoldA("Safety Office Hold Appendix-A") {
        @Override
        public String taskDefKey() {
            return "soHoldA";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_A";
        }
    },

    SOPreApproveB("Safety Office Pre-approve Appendix-B") {
        @Override
        public String taskDefKey() {
            return "soPreApproveB";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_B";
        }
    },

    SOHoldB("Safety Office Hold Appendix-B") {
        @Override
        public String taskDefKey() {
            return "soHoldB";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_B";
        }
    },

    SOPreApproveC("Safety Office Pre-approve Appendix-C") {
        @Override
        public String taskDefKey() {
            return "soPreApproveC";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_C";
        }
    },

    SOHoldC("Safety Office Hold Appendix-C") {
        @Override
        public String taskDefKey() {
            return "soHoldC";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_C";
        }
    },

    SOPreApproveD("Safety Office Pre-approve Appendix-D") {
        @Override
        public String taskDefKey() {
            return "soPreApproveD";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_D";
        }
    },

    SOHoldD("Safety Office Hold Appendix-D") {
        @Override
        public String taskDefKey() {
            return "soHoldD";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_D";
        }
    },
    SOPreApproveE("Safety Office Pre-approve Appendix-E") {
        @Override
        public String taskDefKey() {
            return "soPreApproveE";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_E";
        }
    },

    SOHoldE("Safety Office Hold Appendix-E") {
        @Override
        public String taskDefKey() {
            return "soHoldE";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_E";
        }
    },

    SOPreApproveF("Safety Office Pre-approve Appendix-F") {
        @Override
        public String taskDefKey() {
            return "soPreApproveF";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_F";
        }
    },

    SOHoldF("Safety Office Hold Appendix-F") {
        @Override
        public String taskDefKey() {
            return "soHoldF";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_F";
        }
    },

    SOPreApproveG("Safety Office Pre-approve Appendix-G") {
        @Override
        public String taskDefKey() {
            return "soPreApproveG";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_G";
        }
    },

    SOHoldG("Safety Office Hold Appendix-G") {
        @Override
        public String taskDefKey() {
            return "soHoldG";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_G";
        }
    },

    SOPreApproveI("Safety Office Pre-approve Appendix-I") {
        @Override
        public String taskDefKey() {
            return "soPreApproveI";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_APPROVE_I";
        }
    },

    SOHoldI("Safety Office Hold Appendix-I") {
        @Override
        public String taskDefKey() {
            return "soHoldI";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 10;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_HAZMAT_SAFETY_HOLD_I";
        }
    },

    Redistribute("Redistribute") {
        @Override
        public String taskDefKey() {
            return "redistribute";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 999;
        }

        @Override
        public String getCandidateGroup() {
            return "IACUC_CAN_RE_DISTRIBUTE_COLUMBIA_PROTOCOL";
        }
    },

    AnimalOrder("Animal Order") {
        @Override
        public String taskDefKey() {
            return "animalOrder";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 999;
        }

        @Override
        public String getCandidateGroup() {
            return "";
        }
    },

    M9Reminder("9 Months Reminder") {
        @Override
        public String taskDefKey() {
            return "m9Reminder";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 13;
        }
        @Override
        public String getCandidateGroup() {
            return "";
        }

    },

    M6Reminder("6 Months Reminder") {
        @Override
        public String taskDefKey() {
            return "m6Reminder";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 12;
        }
        @Override
        public String getCandidateGroup() {
            return "";
        }

    },

    M3Reminder("3 Months Reminder") {
        @Override
        public String taskDefKey() {
            return "m3Reminder";
        }

        @Override
        public boolean isDefKey(String def) {
            return this.taskDefKey().equals(def);
        }

        @Override
        public boolean isStatus(String status) {
            return statusName().equals(status);
        }

        @Override
        public int gatewayValue() {
            return 11;
        }
        @Override
        public String getCandidateGroup() {
            return "";
        }

    };

    private String codeName;

    private IacucStatus(String s) {
        this.codeName = s;
    }

    private static final Set<String> KaputSet = new HashSet<String>();

    static {
        KaputSet.add("Release");
        KaputSet.add("UnRelease");
        KaputSet.add("Reject");
        KaputSet.add("Notify");
        //
        KaputSet.add("PreApprove");
        //
        KaputSet.add("ChgApprovalDate");
        KaputSet.add("ChgEffectivDate");
        KaputSet.add("ChgEndDate");
        KaputSet.add("ChgMeetingDate");
        //
        KaputSet.add("FullReviewReq");
        //
        KaputSet.add("HazardsApprove");
        //
        KaputSet.add("SOPreApproveA");
        KaputSet.add("SOPreApproveB");
        KaputSet.add("SOPreApproveC");
        KaputSet.add("SOPreApproveD");
        KaputSet.add("SOPreApproveE");
        KaputSet.add("SOPreApproveF");
        KaputSet.add("SOPreApproveG");
        KaputSet.add("SOPreApproveI");
        KaputSet.add("SOHoldA");
        KaputSet.add("SOHoldB");
        KaputSet.add("SOHoldC");
        KaputSet.add("SOHoldD");
        KaputSet.add("SOHoldE");
        KaputSet.add("SOHoldF");
        KaputSet.add("SOHoldG");
        KaputSet.add("SOHoldI");
        //
        KaputSet.add("VetPreApproveB");
        KaputSet.add("VetPreApproveC");
        KaputSet.add("VetPreApproveE");
        KaputSet.add("VetPreApproveF");
        KaputSet.add("VetHoldB");
        KaputSet.add("VetHoldC");
        KaputSet.add("VetHoldE");
        KaputSet.add("VetHoldF");
    }

    public String statusName() {
        return codeName;
    }

    public boolean isKaput(String name) {
        return KaputSet.contains(name);
    }

    public abstract String taskDefKey();

    public abstract boolean isDefKey(String def);

    public abstract boolean isStatus(String status);

    public abstract int gatewayValue();

    public abstract String getCandidateGroup();
}

