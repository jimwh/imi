package edu.columbia.rascal.business.service.review.iacuc;


import org.activiti.engine.ActivitiIllegalArgumentException;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.DelegateTask;
import org.activiti.engine.delegate.ExecutionListener;
import org.activiti.engine.delegate.TaskListener;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class IacucListener implements TaskListener, ExecutionListener {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(IacucListener.class);

    private static final String AllRvs = "allRvs";
    private static final String hasAppendixA = "hasAppendixA";
    private static final String hasAppendixB = "hasAppendixB";
    private static final String hasAppendixC = "hasAppendixC";
    private static final String hasAppendixD = "hasAppendixD";
    private static final String hasAppendixE = "hasAppendixE";
    private static final String hasAppendixF = "hasAppendixF";
    private static final String hasAppendixG = "hasAppendixG";
    private static final String hasAppendixI = "hasAppendixI";

    private static final String AllAppendicesApproved = "allAppendicesApproved";
    private static final String appendixAApproved = "aApproved";
    private static final String appendixBApproved = "bAApproved";
    private static final String appendixCApproved = "cApproved";
    private static final String appendixDApproved = "dApproved";
    private static final String appendixEApproved = "eApproved";
    private static final String appendixFApproved = "fApproved";
    private static final String appendixGApproved = "gApproved";
    private static final String appendixIApproved = "iApproved";

    private static final String CanRedistribute = "canRedistribute";
    private static final String Redistribute = "redistribute";
    private static final String UndoApproval = "undoApproval";

    private static final Map<String, Boolean> UndoMap = new HashMap<String, Boolean>();

    static {
        UndoMap.put(IacucStatus.ReturnToPI.taskDefKey(), false);
        UndoMap.put(IacucStatus.UndoApproval.taskDefKey(), true);
        UndoMap.put(IacucStatus.FinalApproval.taskDefKey(), false);
    }

    private static final Set<String> RvApprovalCannotDistributeSet = new HashSet<String>();

    static {
        RvApprovalCannotDistributeSet.add(IacucStatus.Rv1Approval.taskDefKey());
        RvApprovalCannotDistributeSet.add(IacucStatus.Rv2Approval.taskDefKey());
        RvApprovalCannotDistributeSet.add(IacucStatus.Rv3Approval.taskDefKey());
        RvApprovalCannotDistributeSet.add(IacucStatus.Rv4Approval.taskDefKey());
        RvApprovalCannotDistributeSet.add(IacucStatus.Rv5Approval.taskDefKey());
    }

    private static final Set<String> RvHoldOrReqFullRvSet = new HashSet<String>();

    static {
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv1Hold.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv1ReqFullReview.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv2Hold.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv2ReqFullReview.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv3Hold.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv3ReqFullReview.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv4Hold.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv4ReqFullReview.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv5Hold.taskDefKey());
        RvHoldOrReqFullRvSet.add(IacucStatus.Rv5ReqFullReview.taskDefKey());
    }

    private static final Map<String, String> SoApproveMap = new HashMap<String, String>();

    static {
        SoApproveMap.put(IacucStatus.SOPreApproveA.taskDefKey(), appendixAApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveB.taskDefKey(), appendixBApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveC.taskDefKey(), appendixCApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveD.taskDefKey(), appendixDApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveE.taskDefKey(), appendixEApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveF.taskDefKey(), appendixFApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveG.taskDefKey(), appendixGApproved);
        SoApproveMap.put(IacucStatus.SOPreApproveI.taskDefKey(), appendixIApproved);
    }

    private static final Set<String> SoHoldSet = new HashSet<String>();

    static {
        SoHoldSet.add(IacucStatus.SOHoldA.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldB.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldC.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldD.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldE.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldF.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldG.taskDefKey());
        SoHoldSet.add(IacucStatus.SOHoldI.taskDefKey());
    }

    /**
     * Task listener will be called  by activity
     */
    @Override
    public void notify(DelegateTask delegateTask) {
    	
    	if( !delegateTask.getProcessDefinitionId().contains("IacucApprovalProcess")) {
    		return;
    	}
    	
        String eventName = delegateTask.getEventName();
        if (EVENTNAME_CREATE.equals(eventName)) {
            onCreate(delegateTask);
        } else if (EVENTNAME_COMPLETE.equals(eventName)) {
            try {
                onComplete(delegateTask);
            } catch (Exception e) {
                throw new ActivitiIllegalArgumentException(e.getMessage());
            }
        }
    }

    private void onCreate(DelegateTask delegateTask) {
        DelegateExecution taskExecution = delegateTask.getExecution();
        String bizKey = taskExecution.getProcessBusinessKey();
        String processId = taskExecution.getProcessInstanceId();
        String taskId = delegateTask.getId();
        String taskDefKey = delegateTask.getTaskDefinitionKey();
        log.info("create: bizKey={}, taskDefKey={}, taskId={}, processId={}",
                bizKey, taskDefKey, taskId, processId);
    }

    private void onComplete(DelegateTask delegateTask) {

        DelegateExecution taskExecution = delegateTask.getExecution();
        String bizKey = taskExecution.getProcessBusinessKey();
        String taskDefKey = delegateTask.getTaskDefinitionKey();

        /*
        don't use it in data migration!!!
        if (!IacucStatus.FinalApproval.isDefKey(taskDefKey)) {
            // header service may be null during unit test
            if (headerService != null)
                headerService.attachSnapshot(bizKey, taskDefKey);
        }
        */

        if (IacucStatus.DistributeReviewer.isDefKey(taskDefKey)) {
            taskExecution.setVariable("hasReviewer", true);
            taskExecution.setVariable(CanRedistribute, true);
        } else if (IacucStatus.Redistribute.isDefKey(taskDefKey)) {
            if (!(Boolean) taskExecution.getVariable(CanRedistribute)) {
                // enforce you can't complete this task
                throw new ActivitiIllegalArgumentException("Illegal action.");
            }
            taskExecution.setVariable(Redistribute, true);
        } 
        else if (UndoMap.get(taskDefKey) != null) {
            // do nothing if user action
            if( taskExecution.getVariable("userClosed")==null) {
        		taskExecution.setVariable(UndoApproval, UndoMap.get(taskDefKey));
        	}
        } 
        else if (RvHoldOrReqFullRvSet.contains(taskDefKey)) {
            taskExecution.setVariable(AllRvs, false);
            taskExecution.setVariable(CanRedistribute, false);
            taskExecution.setVariable(Redistribute, false);
        } else if (RvApprovalCannotDistributeSet.contains(taskDefKey)) {
            taskExecution.setVariable(CanRedistribute, false);
            taskExecution.setVariable(Redistribute, false);
        } else if (SoApproveMap.get(taskDefKey) != null) {
            taskExecution.setVariable(SoApproveMap.get(taskDefKey), true);
            updateAppendixApproveStatus(delegateTask);
        } else if (SoHoldSet.contains(taskDefKey)) {
            taskExecution.setVariable(AllAppendicesApproved, false);
        }
    }


    private void updateAppendixApproveStatus(DelegateTask delegateTask) {

        if (!(Boolean) delegateTask.getVariable(appendixAApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixBApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixCApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixDApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixEApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixFApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixGApproved)) {
            return;
        }
        if (!(Boolean) delegateTask.getVariable(appendixIApproved)) {
            return;
        }

        delegateTask.setVariable(AllAppendicesApproved, true);
    }

    /**
     * Execution listener will be called  by activity
     */
    @Override
    public void notify(DelegateExecution delegateExecution) throws Exception {
    	if( !delegateExecution.getProcessDefinitionId().contains("IacucApprovalProcess")) {
    		return;
    	}
        String eventName = delegateExecution.getEventName();
        if (EVENTNAME_START.equals(eventName)) {
            onStart(delegateExecution);
        }
    }

    // on process start
    private void onStart(DelegateExecution delegateExecution) throws Exception {

        ExecutionEntity thisEntity = (ExecutionEntity) delegateExecution;
        ExecutionEntity superExecEntity = thisEntity.getSuperExecution();
        String eventName = delegateExecution.getEventName();

        if (superExecEntity == null) {
            setUpAppendixApproveStatus(delegateExecution);
            // get the business key of the main process
            log.info("main process: eventName={}, bizKey={}, procDefId={}", eventName, thisEntity.getBusinessKey(), thisEntity.getProcessDefinitionId());
            // used by designatedReviews output
            thisEntity.setVariable(AllRvs, true);
            thisEntity.setVariable("redistribute", false);

        } else {
            // in a sub-process so get the BusinessKey variable set by the caller.
            String key = (String) superExecEntity.getVariable("BusinessKey");
            boolean hasAppendix = (Boolean) superExecEntity.getVariable("hasAppendix");
            log.info("sub-process: eventName={}, bizKey={}, procDefId={}, hasAppendix={}",
                    eventName, key, thisEntity.getProcessDefinitionId(), hasAppendix);
            thisEntity.setVariable("BusinessKey", key);
            // for get task by business key
            thisEntity.setBusinessKey(key);
        }
    }

    private void setUpAppendixApproveStatus(DelegateExecution exe) {
        boolean bool = true;

        if (exe.getVariable(hasAppendixA) == null) {
            exe.setVariable(hasAppendixA, false);
            exe.setVariable(appendixAApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixA)) {
            exe.setVariable(appendixAApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixAApproved, true);
        }

        if (exe.getVariable(hasAppendixB) == null) {
            exe.setVariable(hasAppendixB, false);
            exe.setVariable(appendixBApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixB)) {
            exe.setVariable(appendixBApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixBApproved, true);
        }

        if (exe.getVariable(hasAppendixC) == null) {
            exe.setVariable(hasAppendixC, false);
            exe.setVariable(appendixCApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixC)) {
            exe.setVariable(appendixCApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixCApproved, true);
        }

        if (exe.getVariable(hasAppendixD) == null) {
            exe.setVariable(hasAppendixD, false);
            exe.setVariable(appendixDApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixD)) {
            exe.setVariable(appendixDApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixDApproved, true);
        }

        if (exe.getVariable(hasAppendixE) == null) {
            exe.setVariable(hasAppendixE, false);
            exe.setVariable(appendixEApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixE)) {
            exe.setVariable(appendixEApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixEApproved, true);
        }

        if (exe.getVariable(hasAppendixF) == null) {
            exe.setVariable(hasAppendixF, false);
            exe.setVariable(appendixFApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixF)) {
            exe.setVariable(appendixFApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixFApproved, true);
        }

        if (exe.getVariable(hasAppendixG) == null) {
            exe.setVariable(hasAppendixG, false);
            exe.setVariable(appendixGApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixG)) {
            exe.setVariable(appendixGApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixGApproved, true);
        }

        if (exe.getVariable(hasAppendixI) == null) {
            exe.setVariable(hasAppendixI, false);
            exe.setVariable(appendixIApproved, true);
        } else if ((Boolean) exe.getVariable(hasAppendixI)) {
            exe.setVariable(appendixIApproved, false);
            bool = false;
        } else {
            exe.setVariable(appendixIApproved, true);
        }

        exe.setVariable(AllAppendicesApproved, bool);
        exe.setVariable("hasAppendix", !bool);
        log.info("{}={}", AllAppendicesApproved, bool);
    }
}
