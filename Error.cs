using System;

namespace WhizQ
{
    public class Error
    {
        public string Title { get; set; }
        public string Description { get; set; }
        public dynamic Data { get; set; }
        public DateTime? DateTimeLogged { get; set; }
        public ErrorType ErrorType { get; set; }

        public Error(string title, string description)
        {
            Title = title;
            Description = description;
            ErrorType = ErrorType.Internal;
            DateTimeLogged = DateTime.UtcNow;
        }

        public Error(string title, string description, dynamic data = null)
        {
            Title = title;
            Description = description;
            Data = data;
            DateTimeLogged = DateTime.UtcNow;
            ErrorType = ErrorType.Internal;
        }

        public Error(string title, string description, ErrorType errorType, dynamic data = null)
        {
            Title = title;
            Description = description;
            Data = data;
            DateTimeLogged = DateTime.UtcNow;
            ErrorType = errorType;
        }
    }
}
