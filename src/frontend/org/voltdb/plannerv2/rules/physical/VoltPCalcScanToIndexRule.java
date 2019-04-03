/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.List;
import java.util.Map;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableIndexScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableSequentialScan;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class VoltPCalcScanToIndexRule extends RelOptRule {

    public static final VoltPCalcScanToIndexRule INSTANCE = new VoltPCalcScanToIndexRule();

    private VoltPCalcScanToIndexRule() {
        super(operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltPhysicalTableSequentialScan scan = call.rel(1);
        VoltTable table = scan.getVoltTable();
        assert(table != null);
        return  !table.getCatalogTable().getIndexes().isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltPhysicalCalc calc = call.rel(0);
        final VoltPhysicalTableSequentialScan scan = call.rel(1);

        RexProgram calcProgram = calc.getProgram();

        RexProgram scanProgram = scan.getProgram();
        assert(scanProgram != null);

        // Merge two programs
        RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
                calcProgram,
                scanProgram,
                rexBuilder);

        final Table catTableable = scan.getVoltTable().getCatalogTable();
        final List<Column> columns = CatalogUtil.getSortedCatalogItems(catTableable.getColumns(), "index");

        RexNode filterCondition = calc.getProgram().getCondition();
        RelNode indexScan = null;
        Map<RelNode, RelNode> equiv = new HashMap<>();

        for (Index index : catTableable.getIndexes()) {
            AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    catTableable, columns, filterCondition, mergedProgram, index, SortDirectionType.INVALID);

            if (accessPath != null) {
                // if accessPath.other is not null, need to create a new Filter
                // @TODO Adjust Calc program Condition based on the access path "other" filters
                RelCollation indexCollation;
                try {
                    indexCollation = VoltRexUtil.createIndexCollation(
                            index, catTableable, rexBuilder, mergedProgram);
                } catch (JSONException e) {
                    indexCollation = null;
                }
                Preconditions.checkNotNull(indexCollation);
                final RelNode nextIndexScan = new VoltPhysicalTableIndexScan(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getTable(),
                        scan.getVoltTable(),
                        mergedProgram,
                        index,
                        accessPath,
                        scan.getLimitRexNode(),
                        scan.getOffsetRexNode(),
                        scan.getAggregateRelNode(),
                        scan.getPreAggregateRowType(),
                        scan.getPreAggregateProgram(),
                        scan.getSplitCount(),
                        indexCollation);
                if (indexScan == null) {
                    indexScan = nextIndexScan;
                } else {
                    equiv.put(nextIndexScan, calc);
                }
            }
        }
        if (indexScan != null) {
            call.transformTo(indexScan, equiv);
        }
    }

}
