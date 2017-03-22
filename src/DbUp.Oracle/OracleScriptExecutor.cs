using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Preprocessors;
using DbUp.Engine.Transactions;

namespace DbUp.Oracle
{
    /// <summary>
    /// Oracle scirpt executor
    /// </summary>
    public class OracleScriptExecutor : IScriptExecutor
    {
        private readonly OracleTableJournal journalTable;

        protected virtual IUpgradeLog Logger { get; }
        protected virtual IConnectionManager ConnectionManager { get; }
        protected virtual IEnumerable<IScriptPreprocessor> ScriptPreprocessors { get; }
        protected virtual bool VariablesEnabled { get; }

        /// <summary>
        /// SQLCommand Timeout in seconds. If not set, the default SQLCommand timeout is not changed.
        /// </summary>
        public int? ExecutionTimeoutSeconds { get; set; }

        /// <summary>
        /// Initializes an instance of the <see cref="OracleScriptExecutor"/> class.
        /// </summary>
        /// <param name="connectionManagerProvider">Function that returns connectionManager</param>
        /// <param name="loggerProvider">The logging mechanism.</param>
        /// <param name="variablesEnabledProvider">Function that returns <c>true</c> if variables should be replaced, <c>false</c> otherwise.</param>
        /// <param name="scriptPreprocessors">Script Preprocessors in addition to variable substitution</param>
        /// <param name="table"></param>
        public OracleScriptExecutor(Func<IConnectionManager> connectionManagerProvider, Func<IUpgradeLog> loggerProvider, Func<bool> variablesEnabledProvider, IEnumerable<IScriptPreprocessor> scriptPreprocessors, string table)
        {
            journalTable = new OracleTableJournal(connectionManagerProvider, loggerProvider, table);

            Logger = loggerProvider();
            ConnectionManager = connectionManagerProvider();
            VariablesEnabled = variablesEnabledProvider();
            ScriptPreprocessors = scriptPreprocessors;
        }

        /// <summary>
        /// Executes the specified script against a database at a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        public void Execute(SqlScript script)
        {
            Execute(script, null);
        }

        /// <summary>
        /// Verifies the existence of targeted schema. If schema is not verified, will check for the existence of the dbo schema.
        /// </summary>
        public void VerifySchema()
        {
            // Do nothing
        }

        /// <summary>
        /// Executes the specified script against a database at a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="variables">Variables to replace in the script</param>
        public void Execute(SqlScript script, IDictionary<string, string> variables)
        {
            if (variables == null)
            {
                variables = new Dictionary<string, string>();
            }

            Logger.WriteInformation("Executing SQL Server script '{0}'", script.Name);

            var contents = script.Contents;
            if (VariablesEnabled)
                contents = new VariableSubstitutionPreprocessor(variables).Process(contents);
            contents = (ScriptPreprocessors ?? new IScriptPreprocessor[0])
                .Aggregate(contents, (current, additionalScriptPreprocessor) => additionalScriptPreprocessor.Process(current));

            var scriptStatements = ConnectionManager.SplitScriptIntoCommands(contents);
            var statementIndex = journalTable.GetFailedStatementIndex(script);
            var successfullStatements = scriptStatements.Take(statementIndex).ToList();
            var index = -1;
            if (statementIndex > 0)
            {
                if (!journalTable.ValidateExecutedScript(script, successfullStatements))
                    throw new Exception(
                        $"Invalid change of script {script.Name}. Successfully executed parts of previously failed script has been changed.");

                scriptStatements = scriptStatements.Skip(statementIndex);
                index = statementIndex - 1;
            }

            var executingStatement = string.Empty;
            try
            {
                ConnectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
                {
                    foreach (var statement in scriptStatements)
                    {
                        executingStatement = statement;
                        index++;
                        using (var command = dbCommandFactory())
                        {
                            command.CommandText = statement;
                            if (ExecutionTimeoutSeconds != null)
                                command.CommandTimeout = ExecutionTimeoutSeconds.Value;
                            if (ConnectionManager.IsScriptOutputLogged)
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    Log(reader);
                                }
                            }
                            else
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                });
            }
            catch (SqlException sqlException)
            {
                Logger.WriteInformation("SQL exception has occured in script: '{0}'", script.Name);
                Logger.WriteError("Script block number: {0};    Block line: {1};    Procedure: {2};{5}SQL Exception Number: {3};    Message: {4}{5}", index, sqlException.LineNumber, sqlException.Procedure, sqlException.Number, sqlException.Message, Environment.NewLine);
                if (ConnectionManager.IsScriptOutputLogged)
                {
                    Logger.WriteInformation(executingStatement + Environment.NewLine);
                }
                journalTable.StoreExecutedScript(script, index, sqlException.Message);
                throw;
            }
            catch (DbException sqlException)
            {
                Logger.WriteInformation("DB exception has occured in script: '{0}'", script.Name);
                Logger.WriteError("Script block number: {0}; Error code {1}; Message: {2}", index, sqlException.ErrorCode, sqlException.Message);
                if (ConnectionManager.IsScriptOutputLogged)
                {
                    Logger.WriteInformation(executingStatement + Environment.NewLine);
                }
                journalTable.StoreExecutedScript(script, index, sqlException.Message);
                throw;
            }
            catch (Exception ex)
            {
                Logger.WriteInformation("Exception has occured in script: '{0}'", script.Name);
                Logger.WriteError(ex.ToString());
                if (ConnectionManager.IsScriptOutputLogged)
                {
                    Logger.WriteInformation(executingStatement + Environment.NewLine);
                }
                throw;
            }
        }

        private void Log(IDataReader reader)
        {
            do
            {
                var names = new List<string>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    names.Add(reader.GetName(i));
                }
                var lines = new List<List<string>>();
                while (reader.Read())
                {
                    var line = new List<string>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.GetValue(i);
                        value = value == DBNull.Value ? null : value.ToString();
                        line.Add((string)value);
                    }
                    lines.Add(line);
                }
                var format = "";
                var totalLength = 0;
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var maxLength = lines.Max(l => l[i].Length) + 2;
                    format += " {" + i + ", " + maxLength + "} |";
                    totalLength += (maxLength + 3);
                }
                format = "|" + format;
                totalLength += 1;

                Logger.WriteInformation(new string('-', totalLength));
                Logger.WriteInformation(format, names.ToArray());
                Logger.WriteInformation(new string('-', totalLength));
                foreach (var line in lines)
                {
                    Logger.WriteInformation(format, line.ToArray());
                }
                Logger.WriteInformation(new string('-', totalLength));
                Logger.WriteInformation("\r\n");
            } while (reader.NextResult());
        }
    }
}