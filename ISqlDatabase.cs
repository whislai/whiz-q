using System.Data;
using System.Transactions;

namespace WhizQ
{
    public interface ISqlDatabase : ISqlConfig
    {        
        IDbConnection Connection { get; }

        IDbConnection Open();
    }
}
