package com.raythos.sentilexo.trident.twitter.state;

import storm.trident.state.map.NonTransactionalMap;

public class SentilexoState extends NonTransactionalMap<Long> {
    protected SentilexoState(SentilexoBackingMap sentilexoBackingMap) {
        super(sentilexoBackingMap);
    }
}
