using System.Text;

namespace sqlparser
{
    /// <summary>
    /// Builder class to construct result csv file
    /// </summary>
    class ResultBuilder
    {
        readonly StringBuilder _sbResults;

        public string ResultsText => _sbResults.ToString();

        public ResultBuilder()
        {
            _sbResults = new StringBuilder();
            _sbResults.AppendLine($"Dynamic SQL,DB Name,Stored Proc Name,Action,Entity");
        }

        public void AppendResults(string db, string proc, StatsVisitor visitor)
        {
            if (visitor == null)
            {
                return;
            }

            if (visitor.Tables != null)
            {
                // write body
                foreach (var action in visitor.Tables.Keys)
                {
                    var tables = visitor.Tables[action];
                    foreach (var table in tables)
                    {
                        var qualifiedTableName = table;
                        if (table.Split('.').Length == 1)
                        {
                            qualifiedTableName = $"{db}.{table}";
                        }

                        _sbResults.AppendLine($"{visitor.HasDynamicSql},{db},{proc},{action},{qualifiedTableName}");
                    }
                }
            }

            // write exec records
            foreach (var procName in visitor.ProcNames)
            {
                var qualifiedProcName = procName;
                if (proc.Split('.').Length == 1)
                {
                    qualifiedProcName = $"{db}.{procName}";
                }

                _sbResults.AppendLine($"{visitor.HasDynamicSql},{db},{proc},EXECUTE,{qualifiedProcName}");
            }
        }

        public void AppendErrorResult(string db, string proc, string msg)
        {
            _sbResults.AppendLine($"{false},{db},{proc},{msg},");
        }
    }
}
