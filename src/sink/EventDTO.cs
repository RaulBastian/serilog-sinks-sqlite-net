using System;
using System.Collections.Generic;
using System.Text;

namespace Serilog.Sinks.SQLLiteNET
{
    public class EventDTO
    {
        public int Id { get; set; }
        public string Text { get; set; }
        public string Level { get; set; }
        public string Exception { get; set; }
        public string RenderedMessage { get; set; }

        public string Properties { get; set; }
    }
}
