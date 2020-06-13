using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace sqlparser
{
    /// <summary>
    /// Parse a list of stored procedures and extract SQL entities referenced along with
    /// the SQL operations against each
    /// </summary>
    class Program
    {
        private static HashSet<string> ReferencedStoredProcs { get; set; }

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: sqlparser [code-path] [input-file-path]");
                return;
            }

            var path = args[0];
            Console.WriteLine($"Code path = {path}");
            if (!Directory.Exists(path))
            {
                Console.WriteLine($"Directory {path} does not exist. Please retry.");
                return;
            }

            var procFileName = args[1];
            if (!File.Exists(procFileName))
            {
                Console.WriteLine($"Input file {procFileName} does not exist. Please retry.");
                return;
            }

            var sw = Stopwatch.StartNew();
            sw.Start();

            var runDir = $@"{Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)}";
            ;
            var resultFileName = $@"{runDir}\results.csv";
            Console.WriteLine($"procFile = {procFileName}");

            var resultBuilder = new ResultBuilder();
            var procs = File.ReadAllLines(procFileName)
                .Select(p => p.Trim().Replace("dbo.","").Replace("[","").Replace("]",""))
                .Distinct();
            ReferencedStoredProcs = new HashSet<string>();
            int procIdx = 0;
            int numProcs = procs.Count();
            foreach (var proc in procs)
            {
                Console.WriteLine($"{proc} {++procIdx}/{numProcs}");
                ProcessStoredProc(path, proc, resultBuilder);
            }

            Console.WriteLine($"Writing results to {resultFileName}");
            File.WriteAllText(resultFileName, resultBuilder.ResultsText);

            sw.Stop();
            Console.WriteLine($"Analysis complete. Elapsed time = {sw.Elapsed.TotalSeconds}s");
        }

        /// <summary>
        /// Get stored proc source file and run through sql parser
        /// </summary>
        /// <param name="path"></param>
        /// <param name="proc"></param>
        /// <param name="resultBuilder"></param>
        private static void ProcessStoredProc(string path, string procName, ResultBuilder resultBuilder)
        {
            if (string.IsNullOrWhiteSpace(procName))
            {
                return;
            }

            // get db name from stored proc if there is one
            var parts = procName.Split('.');
            string dbProc = null;
            if (parts?.Length > 1 && parts[0] != "dbo")
            {
                dbProc = parts[0];
            }

            var proc = parts[parts.Length - 1];

            // find source file name for stored proc
            var files = Directory.GetFiles(path, $"{proc}.sql", SearchOption.AllDirectories);
            if (files.Length == 0)
            {
                files = Directory.GetFiles(path, $"*.{proc}.sql", SearchOption.AllDirectories);
            }

            if (files.Length > 0)
            {
                foreach (var file in files)
                {
                    // Run the file through the SQL parser
                    var visitor = ParseFile(file, proc, out var db);
                    resultBuilder.AppendResults(db, proc, visitor);
                    if (visitor?.ProcNames?.Count > 0)
                    {
                        // Parse any referenced stored procs
                        foreach (var referencedProcName in visitor.ProcNames)
                        {
                            if (!ReferencedStoredProcs.Contains(referencedProcName))
                            {
                                Console.WriteLine($"Processing referenced proc: {referencedProcName}");
                                ReferencedStoredProcs.Add(referencedProcName);
                                ProcessStoredProc(path, referencedProcName, resultBuilder);
                            }
                            else
                            {
                                Console.WriteLine($"Already processed referenced proc {referencedProcName}");
                            }
                        }
                    }
                }
            }
            else
            {
                resultBuilder.AppendErrorResult(string.Empty, proc, "Not found");
            }
        }

        /// <summary>
        /// Parse sql fragment and return AST (abstract syntax tree)
        /// </summary>
        /// <param name="procText"></param>
        /// <returns></returns>
        private static (TSqlFragment sqlTree, IList<ParseError> errors) ParseSql(string procText)
        {
            var parser = new TSql150Parser(true);
            using (var textReader = new StringReader(procText))
            {
                var sqlTree = parser.Parse(textReader, out var errors);

                return (sqlTree, errors);
            }
        }

        /// <summary>
        /// Prepare the source file for submitting to the parser
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="procName"></param>
        /// <param name="db"></param>
        /// <returns></returns>
        private static StatsVisitor ParseFile(string filePath, string procName, out string db)
        {
            db = string.Empty;

            var lines = File.ReadAllLines(filePath);

            // extract the stored proc create/alter statement batch
            var sb = new StringBuilder();
            bool started = false;
            for (int i = 0; i < lines.Length; i++)
            {
                var matchUseStatement = Regex.Match(lines[i], "^use\\s*(?<db>\\w*)", RegexOptions.IgnoreCase);
                if (matchUseStatement.Success)
                {
                    db = matchUseStatement.Groups["db"].Value;
                }

                if (Regex.Match(lines[i], "^(create|alter)\\s+proc", RegexOptions.IgnoreCase).Success)
                {
                    started = true;
                }

                if (started)
                {
                    if (lines[i].Trim().ToUpper() == "GO" || 
                        lines[i].Trim().ToUpper() == "'")
                    {
                        break;
                    }
                    sb.Append($"{lines[i]}\r\n");
                }
            }

            var sql = sb.ToString();

            return ProcessSql(sql);
        }

        /// <summary>
        /// Run a sql string through the parser and return a visitor that has traversed and
        /// extracted information from the AST
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private static StatsVisitor ProcessSql(string sql)
        {
            var parsed = ParseSql(sql);
            if (parsed.errors.Any())
            {
                parsed.errors.ToList().ForEach(e => Console.WriteLine(e.Message));
                return null;
            }

            var visitor = new StatsVisitor();
            parsed.sqlTree.Accept(visitor);
            
            return visitor;
        }
    }
}
