package org.apache.ignite.cache.validation;

/*
 * Copyright 2019 JSC SberTech
 */

package com.sbt.dpl.gridgain.affinity;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Имплементация валидатора топологии для кеша.
 * Предотвращает операции с кешем, если топологию покинули все узлы одного ЦОДа
 * <p>
 * Восстановаить возможность работы с кешами можно с помощью добавления/удаления специального узла активатора
 * {@link DPLTopologyValidator#activator(ClusterNode)}
 * <p>
 * <i>Примечание</i>: проверка консистентности конфигурации топологии намеренно пропускается, т.к. она осуществляется перед
 * присоединением узла к топологии.
 */
@SuppressWarnings("unused")
public class DPLTopologyValidator implements TopologyValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DPLTopologyValidator.class);
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Наименование JVM-опции, определящей применение валидации топологии для партиционируемых кешей
     * Класс сохраняется в конфигураци кеша, при откате версии должна быть совместимость.
     * Старая версия имела эту контстанту в JVMOptions, сейчас такого класса нет, так что не имеем права класть
     * в другой класс (в старой версии константы в этом классе не будет). По этому - кладем локально.
     */
    static final String VALIDATOR_START_PROPERTY_NAME = "dpl.topology.validator";

    /** Идентификация дата-центра. */
    static final String DATA_CENTER_IDENTITY = "dc";

    /** Segment activator identity. */
    static final String SEGMENT_ACTIVATOR_IDENTITY = "seg.activator";

    /**
     * Флаг необходимости валидирвать топологию
     */
    public static final boolean USE_VALIDATOR = Boolean.getBoolean(VALIDATOR_START_PROPERTY_NAME);

    /**
     * Имя кеша, инжект выполняется игнайтом
     */
    @CacheNameResource
    private transient String cacheName;

    /**
     * Игнайт, инжект выполняется игнайтом
     */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /**
     * Текущее состояние.
     */
    private transient State state;

    /**
     * {@inheritDoc}
     */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        if (!USE_VALIDATOR)
            return true;

        initIfNeeded(nodes);

        Predicate<ClusterNode> clusterNodePredicate = node ->
            !node.isClient() && node.attribute(DATA_CENTER_IDENTITY) == null;

        if (nodes.stream().anyMatch(clusterNodePredicate)) {

            List<Collection<String>> collect = nodes.stream()
                .filter(clusterNodePredicate)
                .map(ClusterNode::addresses)
                .collect(Collectors.toList());

            LOGGER.error("В топологии обнаружены узлы, неподходящие для обработки данных кеша: "
                + "[имя кеша=[{}], операции с кешем запрещены. Список узлов: [{}]", cacheName, collect);

            return false;
        }

        boolean segmented = segmented(nodes);

        if (!segmented) {
            state = State.VALID;
        } else {
            if (state == State.REPAIRED) {
                return true;
            }

            // Поиск узла, последнего в топологии
            ClusterNode evtNode = evtNode(nodes);

            if (activator(evtNode)) {
                state = State.BEFORE_REPARED;
            } else {
                if (state == State.BEFORE_REPARED) {
                    boolean activatorLeft = true;

                    // Проверка того, что активатор присутствует в текущей топологии.
                    for (ClusterNode node : nodes) {
                        if (node.isClient() && activator(node)) {
                            activatorLeft = false;

                            break;
                        }
                    }

                    if (activatorLeft) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Сегментация грида восстановлена: [cacheName={}]", cacheName);
                        }

                        // Переход в состояние REPAIRED, только если узел-активатор вышел из топологии.
                        state = State.REPAIRED;
                    } else {
                        // no-op
                        // Иначе остаемся в состоянии BEFORE_REPAIRED
                    }
                } else {
                    if (state == State.VALID && LOGGER.isInfoEnabled()) {
                        LOGGER.info("Обнаружена сегментация грида: [cacheName={}]", cacheName);
                    }

                    state = State.NOTVALID;
                }
            }
        }

        return state == State.VALID || state == State.REPAIRED;
    }

    /** */
    private boolean segmented(Collection<ClusterNode> nodes) {

        ClusterNode prev = null;

        for (ClusterNode node : nodes) {
            if (node.isClient()) {
                continue;
            }
            if (prev != null) {
                String prevDC = prev.attribute(DATA_CENTER_IDENTITY);
                String currDC = node.attribute(DATA_CENTER_IDENTITY);

                if (prevDC != null && !prevDC.equals(currDC)) {
                    return false;
                }
            }
            prev = node;
        }
        return true;
    }

    /**
     * @param node Узел.
     * @return {@code True} если эта нода является активатором.
     */
    private boolean activator(ClusterNode node) {
        return node != null
            && node.isClient()
            && node.attribute(SEGMENT_ACTIVATOR_IDENTITY) != null;
    }

    /**
     * Установка начального состояния в случае необходимости
     *
     * @param nodes узлы топологии.
     */
    private void initIfNeeded(Collection<ClusterNode> nodes) {
        byte[] sss;

        if (state != null) {
            return;
        }

        // Номер узла, последнего в текущей топологии.
        long topVer = evtNode(nodes).order();

        while (topVer > 0) {
            Collection<ClusterNode> top = ignite.cluster().topology(topVer);
            topVer--;

            // Остановка, в случае отсутствия весрии топологии
            if (top == null) {
                return;
            }

            boolean segmented = segmented(top);

            // Остановка при валидной топологии.
            if (!segmented) {
                return;
            }

            for (ClusterNode node : top) {
                if (activator(node)) {
                    state = State.REPAIRED;

                    return;
                }
            }
        }
    }

    /**
     * @param nodes узлы топологии.
     * @return Возвращает узел, который последним вошел/вышел в топологию.
     */
    private ClusterNode evtNode(Collection<ClusterNode> nodes) {
        assert !nodes.isEmpty() : "Топология не может быть пустой!";
        final Optional<ClusterNode> max = nodes.stream()
            .max(Comparator.comparingLong(ClusterNode::order));
        return max.get();
    }

    /**
     * Состояния
     */
    private enum State {
        /**
         * Валидное состоятения
         */
        VALID,
        /**
         * Не валидное состояние
         */
        NOTVALID,
        /**
         * Валидное, перед восстановлением
         */
        BEFORE_REPARED,
        /**
         * Валидное состояние, восстановление
         * <p>
         * Изменения в топологии грида валидны в состоянии восстановления.
         */
        REPAIRED
    }
}
