using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace sqlparser
{
    /// <summary>
    /// Visitor class to process TSQL AST nodes
    /// </summary>
    internal class StatsVisitor : TSqlFragmentVisitor
    {
        public Dictionary<string, HashSet<string>> Tables { get; }
        public HashSet<string> SelectTables => GetTables("SELECT");
        public HashSet<string> InsertTables => GetTables("INSERT");
        public HashSet<string> UpdateTables => GetTables("UPDATE");
        public HashSet<string> DeleteTables => GetTables("DELETE");
        public HashSet<string> MergeTables => GetTables("MERGE");
        public HashSet<string> TruncateTables => GetTables("TRUNCATE");
        public HashSet<string> BulkInsertTables => GetTables("BULK INSERT");
        public HashSet<string> UpdateStatisticsTables => GetTables("UPDATE STATISTICS");
        public HashSet<string> UpdateTextTables => GetTables("UPDATETEXT");
        public HashSet<string> UpdateTextBulkTables => GetTables("UPDATETEXT BULK");
            
        public bool HasDynamicSql;
        public HashSet<string> ProcNames { get; }

        public StatsVisitor()
        {
            Tables = new Dictionary<string, HashSet<string>>();
            ProcNames = new HashSet<string>();
        }

        public override void Visit(SelectStatement node)
        {
            var commonTableExpressions = node.WithCtesAndXmlNamespaces?.CommonTableExpressions;
            var cteAliases = new List<string>();
            if (commonTableExpressions != null)
            {
                foreach (var commonTableExpression in commonTableExpressions)
                {
                    var exprName = commonTableExpression.ExpressionName.Value;
                    cteAliases.Add(exprName);
                    var cteQuerySpecification = commonTableExpression.QueryExpression as QuerySpecification;
                    ProcessFromClause(cteQuerySpecification?.FromClause, SqlAction.Select);
                    ProcessWhereClause(cteQuerySpecification?.WhereClause, SqlAction.Select);
                }
            }

            var fromIds = ProcessSelect(node.QueryExpression);

            // Remove aliases
            if (fromIds != null)
            {
                foreach (var fromId in fromIds)
                {
                    if (cteAliases.Contains(fromId.Name))
                    {
                        SelectTables.RemoveWhere(t => t == fromId.Name);
                    }
                }
            }
        }

        public override void Visit(InsertStatement node)
        {
            var insertSpecification = node.InsertSpecification;
            if (insertSpecification == null)
                return;

            var selectInsertSource = insertSpecification.InsertSource as SelectInsertSource;
            var fromIds = ProcessSelect(selectInsertSource?.Select);

            ProcessTarget(insertSpecification.Target, fromIds, InsertTables);
        }

        public override void Visit(UpdateStatement node)
        {
            var updateSpecification = node.UpdateSpecification;
            if (updateSpecification == null)
                return;

            var fromIds = ProcessFromClause(updateSpecification.FromClause, SqlAction.Select);
            ProcessWhereClause(updateSpecification.WhereClause, SqlAction.Select);

            ProcessTarget(updateSpecification.Target, fromIds, UpdateTables);
        }

        public override void Visit(DeleteStatement node)
        {
            var deleteSpecification = node.DeleteSpecification;
            if (deleteSpecification == null)
                return;

            var fromIds = ProcessFromClause(deleteSpecification.FromClause, SqlAction.Select);
            ProcessWhereClause(deleteSpecification.WhereClause, SqlAction.Select);

            ProcessTarget(deleteSpecification.Target, fromIds, DeleteTables);
        }

        public override void Visit(ExecuteStatement node)
        {
            var executeSpecification = node.ExecuteSpecification;
            var executableEntity = executeSpecification?.ExecutableEntity;

            if (executableEntity is ExecutableProcedureReference)
            {
                var procReference = executeSpecification.ExecutableEntity as ExecutableProcedureReference;
                var procName = procReference?.ProcedureReference?.ProcedureReference?.Name?.BaseIdentifier?.Value;
                if (procName != null)
                {
                    if (procName.ToLower() == "sp_executesql")
                    {
                        HasDynamicSql = true;
                    }
                    else
                    {
                        if (procName.ToLower() != "sp_rename")
                        {
                            ProcNames.Add(procName);
                        }
                    }
                }
            }

            if (executableEntity is ExecutableStringList)
            {
                HasDynamicSql = true;
            }
        }

        public override void Visit(ExistsPredicate exists)
        {
            var querySpecification = exists?.Subquery?.QueryExpression as QuerySpecification;
            if (querySpecification != null)
            {
                ProcessTableReferences(querySpecification?.FromClause?.TableReferences, SqlAction.Select);
            }
        }

        public override void Visit(MergeStatement node)
        {
            var mergeSpecification = node.MergeSpecification;

            ProcessTableReferences(new List<TableReference> { mergeSpecification.Target, mergeSpecification.TableReference }, SqlAction.Merge);

            if (mergeSpecification.ActionClauses != null)
            {
                foreach (var actionClause in mergeSpecification.ActionClauses)
                {
                    var action = actionClause.Action;
                    if (action is InsertMergeAction)
                    {
                        ProcessTableReferences(new List<TableReference> { mergeSpecification.Target }, SqlAction.Insert);
                    }
                    else if (action is UpdateMergeAction)
                    {
                        ProcessTableReferences(new List<TableReference> { mergeSpecification.Target }, SqlAction.Update);
                    }
                    else if (action is DeleteMergeAction)
                    {
                        ProcessTableReferences(new List<TableReference> { mergeSpecification.Target }, SqlAction.Delete);
                    }
                }
            }
        }

        public override void Visit(TruncateTableStatement node)
        {
            var tableName = GetVal(node?.TableName);
            TruncateTables.Add(tableName);
        }

        public override void Visit(InsertBulkStatement node)
        {
            var tableName = GetVal(node?.To);
            BulkInsertTables.Add(tableName);
        }

        public override void Visit(UpdateStatisticsStatement node)
        {
            var tableName = GetVal(node.SchemaObjectName);
            UpdateStatisticsTables.Add(tableName);
        }

        public override void Visit(UpdateTextStatement node)
        {
            var identifiers = node?.Column?.MultiPartIdentifier?.Identifiers;
            if (identifiers != null && identifiers.Count > 0)
            {
                var tableName = identifiers[0]?.Value;
                if (node.Bulk)
                {
                    UpdateTextBulkTables.Add(tableName);
                }
                else
                {
                    UpdateTextTables.Add(tableName);
                }
            }
        }

        public void Print(Action<string> print)
        {
            if (HasDynamicSql)
            {
                print("Contains dynamic sql");
            }

            if (SelectTables.Count > 0)
                print($"select:");
            foreach (var table in SelectTables)
            {
                print(table);
            }

            if (InsertTables.Count > 0)
                print($"insert:");
            foreach (var table in InsertTables)
            {
                print(table);
            }

            if (UpdateTables.Count > 0)
                print($"update:");
            foreach (var table in UpdateTables)
            {
                print(table);
            }

            if (DeleteTables.Count > 0)
                print($"delete:");
            foreach (var table in DeleteTables)
            {
                print(table);
            }

            if (MergeTables.Count > 0)
                print($"merge:");
            foreach (var table in MergeTables)
            {
                print(table);
            }

            if (TruncateTables.Count > 0)
                print($"truncate:");
            foreach (var table in TruncateTables)
            {
                print(table);
            }

            if (BulkInsertTables.Count > 0)
                print($"bulk insert:");
            foreach (var table in BulkInsertTables)
            {
                print(table);
            }

            if (UpdateStatisticsTables.Count > 0)
                print($"update statistics:");
            foreach (var table in UpdateStatisticsTables)
            {
                print(table);
            }

            if (UpdateTextTables.Count > 0)
                print($"updatetext:");
            foreach (var table in UpdateTextTables)
            {
                print(table);
            }

            if (UpdateTextBulkTables.Count > 0)
                print($"updatetext bulk:");
            foreach (var table in UpdateTextBulkTables)
            {
                print(table);
            }
        }

        #region Private methods
        private IList<TableIdentifier> ProcessSelect(QueryExpression queryExpression)
        {
            var querySpecification = queryExpression as QuerySpecification;
            if (querySpecification == null)
            {
                var binaryQuerySpecification = queryExpression as BinaryQueryExpression;
                if (binaryQuerySpecification != null)
                {
                    return ProcessBinaryQueryExpression(binaryQuerySpecification);
                }

                return null;
            }

            // process select elements
            foreach (var selectElement in querySpecification.SelectElements)
            {
                if (selectElement is SelectScalarExpression)
                {
                    var expr = (selectElement as SelectScalarExpression).Expression;
                    if (expr is SearchedCaseExpression)
                    {
                        var whenClauses = (expr as SearchedCaseExpression).WhenClauses;
                        foreach (var whenClause in whenClauses)
                        {
                            var whenExpr = whenClause.WhenExpression;
                            var thenExpr = whenClause.ThenExpression;

                            ProcessBooleanExpression(whenExpr);

                            if (thenExpr is ScalarSubquery)
                            {
                                var e = thenExpr as ScalarSubquery;
                                ProcessSelect(e.QueryExpression);
                            }
                        }

                        var elseExpr = (expr as SearchedCaseExpression).ElseExpression as ScalarSubquery;
                        if (elseExpr != null)
                        {
                            ProcessSelect(elseExpr.QueryExpression);
                        }
                    }
                    else if (expr is ScalarSubquery)
                    {
                        var qExpr = (expr as ScalarSubquery).QueryExpression;
                        ProcessSelect(qExpr);
                    }
                    else if (expr is FunctionCall)
                    {
                        ProcessFunctionCall(expr as FunctionCall);
                    }
                }
                else if (selectElement is SelectSetVariable)
                {
                    var expr = (selectElement as SelectSetVariable).Expression;
                    if (expr is ScalarSubquery)
                    {
                        var query = expr as ScalarSubquery;
                        ProcessSelect(query.QueryExpression);
                    }
                }
            }
            

            var fromIds = ProcessFromClause(querySpecification?.FromClause, SqlAction.Select);
            if (querySpecification.WhereClause != null)
            {
                ProcessWhereClause(querySpecification.WhereClause, SqlAction.Select);
            }

            return fromIds;
        }

        private void ProcessFunctionCall(FunctionCall functionCall)
        {
            var func = functionCall.FunctionName.Value;
            if (!SqlUtils.IsBuiltInFunction(func))
                SelectTables.Add(func);
        }

        private void ProcessBooleanExpression(BooleanExpression whenExpr)
        {
            if (whenExpr is BooleanComparisonExpression)
            {
                var e = whenExpr as BooleanComparisonExpression;
                ProcessScalarExpression(e.FirstExpression);
                ProcessScalarExpression(e.SecondExpression);
            }
            else if (whenExpr is BooleanBinaryExpression)
            {
                var firstExpr = (whenExpr as BooleanBinaryExpression).FirstExpression;
                var secondExpr = (whenExpr as BooleanBinaryExpression).SecondExpression;

                ProcessBooleanExpression(firstExpr);
                ProcessBooleanExpression(secondExpr);
            }
            else if (whenExpr is BooleanNotExpression)
            {
                var notExpr = whenExpr as BooleanNotExpression;
                ProcessBooleanExpression(notExpr.Expression);
            }
            else if (whenExpr is BooleanIsNullExpression)
            {
                var expr = whenExpr as BooleanIsNullExpression;
                ProcessScalarExpression(expr.Expression);
            }
            else if (whenExpr is BooleanParenthesisExpression)
            {
                var parExpr = whenExpr as BooleanParenthesisExpression;
                ProcessBooleanExpression(parExpr.Expression);
            }
            else if (whenExpr is BooleanTernaryExpression)
            {
                var terExpr = whenExpr as BooleanTernaryExpression;
            }
            else if (whenExpr is InPredicate)
            {
                var subquery = ((InPredicate) whenExpr).Subquery;
                if (subquery != null)
                {
                    ProcessSelect(subquery.QueryExpression);
                }

                /*var querySpecification = subquery.QueryExpression as QuerySpecification;
                if (querySpecification != null)
                {
                    ProcessFromClause(querySpecification?.FromClause, SqlAction.Select);
                }*/
            }
        }

        private void ProcessScalarExpression(ScalarExpression expr)
        {
            if (expr is ScalarSubquery)
            {
                ProcessSelect((expr as ScalarSubquery).QueryExpression);
            }
            else if (expr is FunctionCall)
            {
                ProcessFunctionCall(expr as FunctionCall);
            }
        }

        private IList<TableIdentifier> ProcessBinaryQueryExpression(BinaryQueryExpression binaryQueryExpression)
        {
            var fromIds = new List<TableIdentifier>();
            if (binaryQueryExpression != null)
            {
                var firstExpr = binaryQueryExpression.FirstQueryExpression;
                var secondExpr = binaryQueryExpression.SecondQueryExpression;

                Func<QueryExpression, IList<TableIdentifier>> processExpr = (expr) =>
                {
                    if (expr is BinaryQueryExpression)
                    {
                        return ProcessBinaryQueryExpression(expr as BinaryQueryExpression);
                    }
                    else if (expr is QuerySpecification)
                    {
                        return ProcessSelect(expr);
                    }
                    return null;
                };

                var firstExprTables = processExpr(firstExpr);
                if (firstExprTables != null && firstExprTables.Count > 0)
                {
                    fromIds.AddRange(firstExprTables);
                }
                var secondExprTables = processExpr(secondExpr);
                if (secondExprTables != null && secondExprTables.Count > 0)
                {
                    fromIds.AddRange(secondExprTables);
                }
            }
            return fromIds;
        }
        private void ProcessTarget(TableReference target, IList<TableIdentifier> fromIds, HashSet<string> tables)
        {
            if (target is NamedTableReference reference)
            {
                string table;
                if (fromIds != null && fromIds.Any(f => f.Alias == reference.SchemaObject.BaseIdentifier.Value))
                {
                    table = fromIds?.Where(f => f.Alias == reference.SchemaObject.BaseIdentifier.Value).First().Name;
                }
                else
                {
                    table = GetVal(reference.SchemaObject);
                }

                if (!string.IsNullOrWhiteSpace(table))
                {
                    tables.Add(table);
                }
            }
        }

        private void ProcessWhereClause(WhereClause whereClause, SqlAction action)
        {
            var searchCondition = whereClause?.SearchCondition;
            ProcessBooleanExpression(searchCondition);
            /*if (searchCondition is InPredicate)
            {
                var subquery = ((InPredicate) searchCondition).Subquery;
                var querySpecification = subquery.QueryExpression as QuerySpecification;
                ProcessFromClause(querySpecification?.FromClause, action);
            }*/
        }

        private IList<TableIdentifier> ProcessFromClause(FromClause fromClause, SqlAction action)
        {
            var tableReferences = fromClause?.TableReferences;
            if (tableReferences != null)
            {
                return ProcessTableReferences(tableReferences, action);
            }

            return null;
        }

        private IList<TableIdentifier> ProcessJoin(QualifiedJoin joinRef, SqlAction action)
        {
            var identifiers = new List<TableIdentifier>();
            
            if (joinRef != null)
            {
                HashSet<string> tables;
                switch (action)
                {
                    case SqlAction.Delete:
                        tables = DeleteTables;
                        break;
                    default: 
                        tables = SelectTables;
                        break;
                }

                identifiers.AddRange(ProcessTableReference(joinRef.FirstTableReference, action, tables));

                identifiers.AddRange(ProcessTableReference(joinRef.SecondTableReference, SqlAction.Select));
            }
            return identifiers;
        }

        private IList<TableIdentifier> ProcessJoin(UnqualifiedJoin joinRef, SqlAction action)
        {
            return ProcessTableReferences(new List<TableReference> { joinRef.FirstTableReference, joinRef.SecondTableReference}, action);
        }

        private IList<TableIdentifier> ProcessTableReference(TableReference tRef, SqlAction action, HashSet<string> tablesParam = null)
        {
            return ProcessTableReferences(new List<TableReference> { tRef }, action, tablesParam);
        }

        private IList<TableIdentifier> ProcessTableReferences(IList<TableReference> tRefs, SqlAction action, HashSet<string> tablesParam = null)
        {
            var identifiers = new List<TableIdentifier>();
            
            if (tRefs != null)
            {
                foreach (var tref in tRefs)
                {
                    var namedRef = tref as NamedTableReference;
                    if (namedRef != null)
                    {
                        var tables = tablesParam ?? GetTables(action);
                        var identifier = new TableIdentifier(string.Empty, string.Empty);
                        if (!string.IsNullOrWhiteSpace(namedRef?.Alias?.Value))
                        {
                            identifier.Alias = namedRef?.Alias?.Value;
                        }
                        var table = GetVal(namedRef); 
                        identifier.Name = table;
                        identifiers.Add(identifier);
                        if (table != null)
                            tables.Add(table);
                    }

                    var joinRef = tref as QualifiedJoin;
                    if (joinRef != null)
                    {
                        var joinIds = ProcessJoin(joinRef, action);
                        identifiers.AddRange(joinIds);
                    }

                    var uqJoinRef = tref as UnqualifiedJoin;
                    if (uqJoinRef != null)
                    {
                        var joinIds = ProcessJoin(uqJoinRef, action);
                        identifiers.AddRange(joinIds);
                    }

                    var pivotedRef = tref as PivotedTableReference;
                    if (pivotedRef != null)
                    {
                        ProcessTableReference(pivotedRef.TableReference, SqlAction.Select);
                    }

                    if (tref is QueryDerivedTable)
                    {
                        var queryExpr =
                            (tref as QueryDerivedTable).QueryExpression;
                        if (queryExpr != null)
                        {
                            ProcessSelect(queryExpr);
                        }
                    }
                    else
                    if (tref is SchemaObjectFunctionTableReference)
                    {
                        var schemaObject = tref as SchemaObjectFunctionTableReference;
                        var function = GetVal(schemaObject.SchemaObject);
                        var id = new TableIdentifier(schemaObject.Alias?.Value, function);
                        identifiers.Add(id);
                        if (!string.IsNullOrWhiteSpace(function))
                            SelectTables.Add(function);
                    }
                }
            }
            return identifiers;
        }

        private HashSet<string> GetTables(SqlAction action)
        {
            HashSet<string> tables = null;
            switch (action)
            {
                case SqlAction.Insert:
                    tables = InsertTables;
                    break;
                case SqlAction.Update:
                    tables = UpdateTables;
                    break;
                case SqlAction.Merge:
                    tables = MergeTables;
                    break;
                case SqlAction.Delete:
                    tables = DeleteTables;
                    break;
                case SqlAction.Truncate:
                    tables = TruncateTables;
                    break;
                case SqlAction.BulkInsert:
                    tables = BulkInsertTables;
                    break;
                default:
                    tables = SelectTables;
                    break;
            }
            return tables;
        }

        private string GetVal(NamedTableReference namedRef)
        {
            return GetVal(namedRef?.SchemaObject);
        }

        private string GetVal(SchemaObjectName schemaObject)
        {
            var val = schemaObject?.BaseIdentifier?.Value;
            if (string.IsNullOrWhiteSpace(val))
                return string.Empty;
            var db = schemaObject.DatabaseIdentifier?.Value;

            return (val.Contains("sysobjects") || val.Contains("objects") || val.Contains("#")) ? null : (!string.IsNullOrEmpty(db) ? $"{db}.{val}" : val);
        }

        private HashSet<string> GetTables(string key) 
        {
            if (!Tables.ContainsKey(key))
            {
                var tables = new HashSet<string>();
                Tables.Add(key, tables);
            }

            return Tables[key];
        }

        #endregion
    }
}
