package edu.columbia.rascal.business.service;

import edu.columbia.rascal.batch.iacuc.OldStatus;

import java.util.List;

public class KaputWorker implements Runnable {
    private final Migrator migrator;
    private final List<OldStatus> list;

    public KaputWorker(Migrator migrator, List<OldStatus> list) {
        this.migrator=migrator;
        this.list=list;
    }

    @Override
    public void run() {
        migrator.importKaput(list);
    }
}
