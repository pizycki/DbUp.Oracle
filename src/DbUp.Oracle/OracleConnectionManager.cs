using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DbUp.Engine.Transactions;
using Oracle.ManagedDataAccess.Client;

namespace DbUp.Oracle
{
    /// <summary>
    /// Default oracle connection manager based on Oracle ODP.NET
    /// </summary>
    public class OracleConnectionManager : DatabaseConnectionManager
    {
        /// <summary>
        /// Creates a new Oracle database connection.
        /// </summary>
        /// <param name="connectionString">The Oracle connection string.</param>
        public OracleConnectionManager(string connectionString) : base(new DelegateConnectionFactory(l => new OracleConnection(connectionString)))
        {
        }

        /// <summary>
        /// Oracle statements seprator is / (slash)
        /// </summary>
        public override IEnumerable<string> SplitScriptIntoCommands(string scriptContents)
        {
            var scriptStatements =
                Regex.Split(scriptContents, "/\r*$", RegexOptions.Multiline)
                    .Select(x => x.Trim())
                    .Where(x => x.Length > 0)
                    .ToArray();

            return scriptStatements;
        }
    }
}