/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannerv2.rules.physical;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.rel.physical.*;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.plannerv2.utils.VoltRelUtil;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class VoltPSortScanToIndexRule extends RelOptRule {

    public static final VoltPSortScanToIndexRule INSTANCE_1 =
            new VoltPSortScanToIndexRule(operand(VoltPhysicalSort.class,
                operand(VoltPhysicalTableScan.class, none())),
                    "SortScanToIndexRule_1");

    public static final VoltPSortScanToIndexRule INSTANCE_2 =
            new VoltPSortScanToIndexRule(operand(VoltPhysicalSort.class,
                operand(VoltPhysicalCalc.class,
                        operand(VoltPhysicalTableSequentialScan.class, none()))),
                    "SortScanToIndexRule_2");

    private VoltPSortScanToIndexRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltPhysicalTableSequentialScan scan = (call.rels.length == 2) ?
                call.rel(1) : call.rel(2);
        VoltTable table = scan.getVoltTable();
        assert(table != null);
        return !table.getCatalogTable().getIndexes().isEmpty();
    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalSort sort = call.rel(0);
        RelCollation origSortCollation = sort.getCollation();
        assert(!RelCollations.EMPTY.equals(origSortCollation) &&
                sort.fetch == null &&
                sort.offset == null);

        RelCollation scanSortCollation = null;
        VoltPhysicalCalc calc = null;
        VoltPhysicalTableSequentialScan scan = null;
        if (call.rels.length == 2) {
            scan = call.rel(1);
            scanSortCollation = origSortCollation;
        } else {
            calc = call.rel(1);
            scanSortCollation = VoltRelUtil.sortCollationCalcTranspose(origSortCollation, calc);
            if (RelCollations.EMPTY.equals(scanSortCollation)) {
                return;
            }
            scan = call.rel(2);
        }
        Table catTable = scan.getVoltTable().getCatalogTable();

        RexBuilder builder = scan.getCluster().getRexBuilder();
        RexProgram program = scan.getProgram();
        assert(program != null);
        RelNode equivRel = null;
        Map<RelNode, RelNode> equivMap = new HashMap<>();

        for (Index index : catTable.getIndexes()) {
            if (!index.getPredicatejson().isEmpty()) {
                // this is apartial index and it can not be considered here
                continue;
            }
            RelCollation indexCollation =
                    VoltRexUtil.createIndexCollation(index, catTable, builder, program);
            SortDirectionType sortDirection =
                    VoltRexUtil.areCollationsCompatible(scanSortCollation, indexCollation);
            //@TODO Cutting corner here. Should probably use something similar to
            // the SubPlanAssembler.WindowFunctionScoreboard
            if (SortDirectionType.INVALID != sortDirection) {
                AccessPath accessPath = new AccessPath(
                        index,
                        // With no index expression, the lookup type will be ignored and
                        // the sort direction will determine the scan direction;
                        IndexLookupType.EQ,
                        sortDirection,
                        true);
                VoltPhysicalTableIndexScan indexScan = new VoltPhysicalTableIndexScan(
                        scan.getCluster(),
                        // Need to preserve sort collation trait
                        scan.getTraitSet().replace(scanSortCollation),
                        scan.getTable(),
                        scan.getVoltTable(),
                        scan.getProgram(),
                        index,
                        accessPath,
                        scan.getLimitRexNode(),
                        scan.getOffsetRexNode(),
                        scan.getAggregateRelNode(),
                        scan.getPreAggregateRowType(),
                        scan.getPreAggregateProgram(),
                        scan.getSplitCount(),
                        indexCollation);

                RelNode result = null;
                if (calc == null) {
                    result = indexScan;
                } else {
                    // The new Calc collation must match the original Sort collation
                    result = calc.copy(
                            calc.getTraitSet().replace(origSortCollation),
                            indexScan,
                            calc.getProgram(),
                            calc.getSplitCount());
                }

                if (equivRel == null) {
                    equivRel = result;
                } else {
                    equivMap.put(result, sort);
                }
            }
        }
        if (equivRel != null) {
            call.transformTo(equivRel, equivMap);
        }

    }

}
