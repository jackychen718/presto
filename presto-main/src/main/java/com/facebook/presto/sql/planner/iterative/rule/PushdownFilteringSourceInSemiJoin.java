package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Interpreters;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.VariableResolver;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.Row;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.evaluateConstantRowExpression;
import static com.facebook.presto.sql.planner.plan.Patterns.SemiJoin.filteringSource;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PushdownFilteringSourceInSemiJoin
    implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin().with(filteringSource().matching(project()));


    private final FunctionAndTypeManager functionAndTypeManager;

    public PushdownFilteringSourceInSemiJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        PlanNode source = semiJoinNode.getSource();
        Optional<PlanNode> optionalPlanNode = context.getLookup().resolveGroup(semiJoinNode.getFilteringSource()).findFirst();
        ImmutableList.Builder<ProjectNode> projectBuilder = ImmutableList.builder();
        ValuesNode valuesNode=null;
        //find the continuous ProjectNode in filteringSource
        while(optionalPlanNode.isPresent())
        {
            if(optionalPlanNode.get() instanceof ProjectNode){
                ProjectNode projectNode = (ProjectNode)optionalPlanNode.get();
                if(projectNode.getOutputVariables().size()>1)
                {
                    return Result.empty();
                }
                projectBuilder.add(projectNode);
                optionalPlanNode = context.getLookup().resolveGroup(projectNode.getSource()).findFirst();
            }
            else if(optionalPlanNode.get() instanceof ValuesNode){
                valuesNode = (ValuesNode)optionalPlanNode.get();
                break;
            }
            else{
                return Result.empty();
            }
        }
        ImmutableList<ProjectNode> listOfProjectNodes = projectBuilder.build().reverse();
        if(valuesNode==null || valuesNode.getOutputVariables().size() > 1 || listOfProjectNodes.isEmpty()){
            return Result.empty();
        }

        List<List<Object>> rowValues = valuesNode.getRows().stream()
                .map(row -> row.stream().map(element -> evaluateConstantRowExpression(element,functionAndTypeManager,context.getSession().toConnectorSession())).collect(toList()))
                .collect(toList());
        List<VariableReferenceExpression> valuesOutputVariables = valuesNode.getOutputVariables();

        //evaluate value from ValuesNode util the last ProjectNode
        for(ProjectNode projectNode : listOfProjectNodes){
            Collection<RowExpression> projectRowExpressions  = projectNode.getAssignments().getExpressions();
            List<VariableReferenceExpression> finalValuesOutputVariables = valuesOutputVariables;
            rowValues = rowValues.stream().map( row -> {
                Map<String, Object> valuesForResolver = new HashMap<>();
                IntStream.range(0, row.size()).forEach(index -> valuesForResolver.put(finalValuesOutputVariables.get(index).getName(), row.get(index)));
                VariableResolver variableResolver = new Interpreters.LambdaVariableResolver(valuesForResolver);
                List<Object> rowValue = new ArrayList<>();
                for (RowExpression rowExpression : projectRowExpressions) {
                    RowExpressionInterpreter interpreter = new RowExpressionInterpreter(rowExpression,functionAndTypeManager,context.getSession().toConnectorSession(),OPTIMIZED);
                    rowValue.add(interpreter.optimize(variableResolver));
                }
                return rowValue;
            }).collect(toList());
            valuesOutputVariables = new ArrayList<>(projectNode.getAssignments().getVariables());
        }

        //Transform the last ProjectNode to ValuesNode
        List<Type> types = valuesOutputVariables.stream().map(RowExpression::getType).collect(toList());
        List<List<RowExpression>> rows = rowValues.stream().map(row ->{
            List<RowExpression> listRowExpression = new ArrayList<>();
            IntStream.range(0,row.size()).forEach(index-> listRowExpression.add(new ConstantExpression(row.get(index),types.get(index))));
            return listRowExpression;
        }).collect(toList());
        ValuesNode newFilteringSource = new ValuesNode(source.getSourceLocation(), context.getIdAllocator().getNextId(), valuesOutputVariables, rows,Optional.empty());

        SemiJoinNode replacement = new SemiJoinNode(semiJoinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                source,
                newFilteringSource,
                semiJoinNode.getSourceJoinVariable(),
                semiJoinNode.getFilteringSourceJoinVariable(),
                semiJoinNode.getSemiJoinOutput(),
                semiJoinNode.getSourceHashVariable(),
                semiJoinNode.getFilteringSourceHashVariable(),
                semiJoinNode.getDistributionType(),
                semiJoinNode.getDynamicFilters());
        return Result.ofPlanNode(replacement);
    }
}
