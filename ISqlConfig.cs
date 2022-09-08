using System;
using System.Collections.Generic;
using System.Text;

namespace WhizQ
{
    public interface ISqlConfig
    {
        DatabaseProvider DatabaseProvider { get; }
        string ConnectionString { get; }
    }
}
