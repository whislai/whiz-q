using System;
using System.Collections.Generic;
using System.Transactions;
using Dapper;

namespace WhizQ
{
    public interface ISql
    {
        ISqlDatabase SqlDatabase { get; }
        bool? DisableTransactionScope { get; }
        IsolationLevel? IsolationLevel { get; }
        bool? AutoCommit { get; }
        SqlCommand SqlCommand { get; }
        string RootTable { get; } //Top level parent table name with potential child tables
        List<Criteria> CriteriaList { get; } //Criteria list for Sql
        List<Criteria> RootFieldList { get; } //Root fields to be included for read/write query
        List<Criteria> TableFieldList { get; } //Different tables & fields to be included for read/write query
        DynamicParameters Parameters { get; }
        string SQL { get; }
        int Count { get; } //SQL execution count OR count of records based on query conditions
        int Result { get; } //SQL execution result, data will be committed at the end of transaction scope when Count=Result        
        string SqlError { get; }
        List<Error> Errors { get; } //List of errors, i.e. Internal (system debug and logging only) OR (to be shown at frontend)
        List<dynamic> DynamicList { get; }
    }
}
