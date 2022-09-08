using System;
using System.Reflection;

namespace WhizQ
{
    public class Criteria
    {
        public Bracket Bracket { get; set; } = Bracket.None;
        public Logic Logic { get; set; } = Logic.None;
        public Pipe Pipe { get; set; } = Pipe.None;
        public string Table { get; set; } = "";
        public bool IsChild { get; set; } = false;
        public string Column { get; set; } = "";
        public string Alias { get; set; } = "";
        public PropertyInfo PropertyInfo { get; set; }
        public string Operator { get; set; }
        public dynamic Value { get; set; }
        public string[] UpdateExpression { get; set; }
        public bool IsId { get; set; } = false;
        public bool IsIdentity { get; set; } = false;
        public bool GroupBy { get; set; } = false;
        public SortOrder SortOrder { get; set; } = SortOrder.None;
        public string SortCase { get; set; } = "";
        public int Limit { get; set; } = 0;
        public int Page { get; set; } = 0;

        public Criteria()
        {

        }
    }
}
